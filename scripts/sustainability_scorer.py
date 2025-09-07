# scripts/sustainability_scorer.py
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATABASE_PATH

class SustainabilityScorer:
    def __init__(self):
        # Component weights (total = 100%)
        self.weights = {
            'soil_health': 0.30,
            'water_efficiency': 0.25,
            'crop_health': 0.20,
            'risk_management': 0.15,
            'resource_optimization': 0.10
        }
        
        print("🌱 Sustainability Scorer initialized!")
    
    def calculate_soil_health_score(self, farm_id, days_back=7):
        """Calculate soil health from recent sensor data"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            
            query = '''
            SELECT soil_ph, nitrogen, phosphorus, potassium, soil_moisture
            FROM soil_sensor_data 
            WHERE farm_id = ? AND datetime(timestamp) >= datetime('now', '-{} days')
            ORDER BY timestamp DESC LIMIT 10
            '''.format(days_back)
            
            df = pd.read_sql_query(query, conn, params=(farm_id,))
            conn.close()
            
            if df.empty:
                return 50  # Default score
            
            # Calculate component scores
            avg_ph = df['soil_ph'].mean()
            avg_moisture = df['soil_moisture'].mean()
            avg_nitrogen = df['nitrogen'].mean()
            
            # pH score (optimal 6.0-7.5)
            if 6.0 <= avg_ph <= 7.5:
                ph_score = 100
            elif 5.5 <= avg_ph < 6.0 or 7.5 < avg_ph <= 8.0:
                ph_score = 80
            else:
                ph_score = 50
            
            # Moisture score (optimal 40-70%)
            if 40 <= avg_moisture <= 70:
                moisture_score = 100
            elif 30 <= avg_moisture < 40 or 70 < avg_moisture <= 80:
                moisture_score = 80
            else:
                moisture_score = 50
            
            # NPK score (simplified)
            npk_score = min(100, (avg_nitrogen / 100) * 100)
            
            # Overall soil health score
            soil_score = (ph_score + moisture_score + npk_score) / 3
            
            return round(soil_score, 1)
            
        except Exception as e:
            print(f"❌ Error calculating soil health: {e}")
            return 50
    
    def calculate_water_efficiency_score(self, farm_id, days_back=7):
        """Calculate water efficiency"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            
            # Get rainfall and soil moisture
            weather_query = '''
            SELECT rainfall_1h FROM weather_data 
            WHERE farm_id = ? AND datetime(timestamp) >= datetime('now', '-{} days')
            '''.format(days_back)
            
            sensor_query = '''
            SELECT soil_moisture FROM soil_sensor_data 
            WHERE farm_id = ? AND datetime(timestamp) >= datetime('now', '-{} days')
            '''.format(days_back)
            
            weather_df = pd.read_sql_query(weather_query, conn, params=(farm_id,))
            sensor_df = pd.read_sql_query(sensor_query, conn, params=(farm_id,))
            conn.close()
            
            if weather_df.empty or sensor_df.empty:
                return 70  # Default score
            
            total_rainfall = weather_df['rainfall_1h'].sum()
            avg_moisture = sensor_df['soil_moisture'].mean()
            
            # Water efficiency logic
            if total_rainfall < 20:  # Low rainfall
                if avg_moisture > 40:
                    efficiency_score = 90  # Good water retention
                else:
                    efficiency_score = 50
            else:  # Good rainfall
                if 40 <= avg_moisture <= 70:
                    efficiency_score = 85
                else:
                    efficiency_score = 60
            
            return round(efficiency_score, 1)
            
        except Exception as e:
            print(f"❌ Error calculating water efficiency: {e}")
            return 70
    
    def calculate_risk_management_score(self, farm_id, days_back=30):
        """Calculate risk management from alerts"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            
            fire_query = '''
            SELECT risk_level FROM fire_alerts 
            WHERE farm_id = ? AND datetime(timestamp) >= datetime('now', '-{} days')
            ORDER BY timestamp DESC LIMIT 1
            '''.format(days_back)
            
            fire_df = pd.read_sql_query(fire_query, conn, params=(farm_id,))
            conn.close()
            
            risk_score = 100  # Start with perfect score
            
            if not fire_df.empty:
                fire_risk = fire_df.iloc[0]['risk_level']
                if fire_risk == 'critical':
                    risk_score -= 40
                elif fire_risk == 'high':
                    risk_score -= 25
                elif fire_risk == 'moderate':
                    risk_score -= 10
            
            return max(0, risk_score)
            
        except Exception as e:
            print(f"❌ Error calculating risk management: {e}")
            return 80
    
    def calculate_overall_score(self, farm_id):
        """Calculate comprehensive sustainability score"""
        print(f"🧮 Calculating sustainability score for {farm_id}")
        
        # Get component scores
        soil_health = self.calculate_soil_health_score(farm_id)
        water_efficiency = self.calculate_water_efficiency_score(farm_id)
        risk_management = self.calculate_risk_management_score(farm_id)
        
        # Simulated scores for components we don't have full data for yet
        crop_health = 75  # Would come from satellite analysis
        resource_optimization = 80  # Would come from usage analysis
        
        components = {
            'soil_health': soil_health,
            'water_efficiency': water_efficiency,
            'crop_health': crop_health,
            'risk_management': risk_management,
            'resource_optimization': resource_optimization
        }
        
        # Calculate weighted score
        weighted_score = 0
        for component, score in components.items():
            weighted_score += score * self.weights[component]
        
        # Scale to 0-1000
        final_score = int(weighted_score * 10)
        
        # Determine grade
        if final_score >= 800:
            grade = 'Excellent'
        elif final_score >= 700:
            grade = 'Good'
        elif final_score >= 600:
            grade = 'Fair'
        else:
            grade = 'Poor'
        
        # Generate recommendations
        recommendations = self.generate_recommendations(components)
        
        return {
            'farm_id': farm_id,
            'overall_score': final_score,
            'grade': grade,
            'components': components,
            'recommendations': recommendations,
            'calculated_at': datetime.now().isoformat()
        }
    
    def generate_recommendations(self, components):
        """Generate actionable recommendations"""
        recommendations = []
        
        if components['soil_health'] < 70:
            recommendations.append("🌱 Improve soil health through organic matter addition")
        
        if components['water_efficiency'] < 70:
            recommendations.append("💧 Implement water conservation techniques")
        
        if components['risk_management'] < 70:
            recommendations.append("🔥 Enhance disaster preparedness measures")
        
        if len(recommendations) == 0:
            recommendations.append("✅ Excellent work! Continue current practices")
        
        return recommendations
    
    def save_score(self, analysis):
        """Save sustainability score to database"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO sustainability_scores 
            (farm_id, timestamp, overall_score, grade, soil_health_score,
             water_efficiency_score, crop_health_score, risk_management_score,
             recommendations)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                analysis['farm_id'], analysis['calculated_at'],
                analysis['overall_score'], analysis['grade'],
                analysis['components']['soil_health'],
                analysis['components']['water_efficiency'],
                analysis['components']['crop_health'],
                analysis['components']['risk_management'],
                json.dumps(analysis['recommendations'])
            ))
            
            conn.commit()
            conn.close()
            
            print("💾 Sustainability score saved!")
            return True
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False

def test_sustainability_scorer():
    scorer = SustainabilityScorer()
    
    analysis = scorer.calculate_overall_score('FARM001')
    
    print("\n🌱 SUSTAINABILITY ANALYSIS:")
    print("=" * 50)
    print(f"   🏠 Farm: {analysis['farm_id']}")
    print(f"   🌟 Overall Score: {analysis['overall_score']}/1000")
    print(f"   📊 Grade: {analysis['grade']}")
    
    print("\n📋 COMPONENT SCORES:")
    for component, score in analysis['components'].items():
        emoji = "💚" if score >= 80 else "🟡" if score >= 60 else "🔴"
        print(f"   {emoji} {component.replace('_', ' ').title()}: {score}/100")
    
    print("\n💡 RECOMMENDATIONS:")
    for i, rec in enumerate(analysis['recommendations'], 1):
        print(f"   {i}. {rec}")
    
    scorer.save_score(analysis)

if __name__ == "__main__":
    test_sustainability_scorer()
