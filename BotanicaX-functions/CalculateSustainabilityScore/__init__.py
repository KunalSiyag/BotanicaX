import azure.functions as func
import logging
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared_code'))
from database_helper import CosmosDBHelper

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info(f'BotanicaX Sustainability Score calculation executed at: {utc_timestamp}')
    
    try:
        db = CosmosDBHelper()
        calculator = SustainabilityCalculator(db)
        
        # Get all active farms
        farms = get_active_farms()
        
        for farm in farms:
            try:
                farm_id = farm['id']
                logging.info(f"Calculating sustainability score for farm {farm_id}")
                
                score_data = calculator.calculate_comprehensive_score(farm_id)
                
                if score_data:
                    # Store in Cosmos DB
                    db.insert_item('sustainability_scores', score_data)
                    logging.info(f"Sustainability score calculated for farm {farm_id}: {score_data['overall_score']}")
                
            except Exception as e:
                logging.error(f"Error calculating score for farm {farm['id']}: {e}")
        
        logging.info("Sustainability score calculation completed")
        
    except Exception as e:
        logging.error(f"Error in sustainability calculation: {e}")
        raise

class SustainabilityCalculator:
    def __init__(self, db: CosmosDBHelper):
        self.db = db
        
        # Component weights (must sum to 1.0)
        self.weights = {
            'soil_health': 0.35,
            'water_efficiency': 0.25,
            'air_quality': 0.20,
            'crop_health': 0.10,
            'risk_management': 0.10
        }
    
    def calculate_comprehensive_score(self, farm_id: str):
        """Calculate comprehensive sustainability score"""
        
        try:
            # Get component scores
            soil_score = self._calculate_soil_health_score(farm_id)
            water_score = self._calculate_water_efficiency_score(farm_id)
            air_score = self._calculate_air_quality_score(farm_id)
            crop_score = self._calculate_crop_health_score(farm_id)
            risk_score = self._calculate_risk_management_score(farm_id)
            
            # Component scores dictionary
            components = {
                'soil_health': soil_score,
                'water_efficiency': water_score,
                'air_quality': air_score,
                'crop_health': crop_score,
                'risk_management': risk_score
            }
            
            # Calculate weighted overall score
            overall_score = 0
            for component, score in components.items():
                overall_score += score * self.weights[component]
            
            # Scale to 0-1000
            final_score = int(overall_score * 10)
            
            # Determine grade
            grade = self._determine_grade(final_score)
            
            score_data = {
                'farm_id': farm_id,
                'overall_score': final_score,
                'grade': grade,
                'components': components,
                'calculation_method': 'weighted_average',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return score_data
            
        except Exception as e:
            logging.error(f"Error calculating sustainability score for {farm_id}: {e}")
            return None
    
    def _calculate_soil_health_score(self, farm_id: str):
        """Calculate soil health component score"""
        
        # Get recent soil sensor data (last 7 days)
        query = """
        SELECT * FROM c 
        WHERE c.farm_id = @farm_id AND c.sensor_type = 'soil'
        AND c.timestamp >= @time_window
        ORDER BY c.timestamp DESC
        """
        
        time_window = (datetime.utcnow() - timedelta(days=7)).isoformat()
        parameters = [
            {"name": "@farm_id", "value": farm_id},
            {"name": "@time_window", "value": time_window}
        ]
        
        soil_data = self.db.query_items('sensor_data', query, parameters)
        
        if not soil_data:
            return 70  # Default score
        
        # Calculate averages
        avg_ph = sum(float(item.get('soil_ph', 7)) for item in soil_data) / len(soil_data)
        avg_moisture = sum(float(item.get('soil_moisture', 50)) for item in soil_data) / len(soil_data)
        avg_nitrogen = sum(float(item.get('nitrogen', 50)) for item in soil_data) / len(soil_data)
        
        # Score calculations (0-100)
        ph_score = self._score_ph_level(avg_ph)
        moisture_score = self._score_moisture_level(avg_moisture)
        nitrogen_score = self._score_nitrogen_level(avg_nitrogen)
        
        # Weighted soil health score
        soil_score = (ph_score * 0.4 + moisture_score * 0.4 + nitrogen_score * 0.2)
        
        return round(soil_score, 1)
    
    def _calculate_water_efficiency_score(self, farm_id: str):
        """Calculate water efficiency score"""
        
        # Get weather data for rainfall analysis
        query = """
        SELECT * FROM c 
        WHERE c.farm_id = @farm_id 
        AND c.timestamp >= @time_window
        ORDER BY c.timestamp DESC
        """
        
        time_window = (datetime.utcnow() - timedelta(days=7)).isoformat()
        parameters = [
            {"name": "@farm_id", "value": farm_id},
            {"name": "@time_window", "value": time_window}
        ]
        
        weather_data = self.db.query_items('weather_data', query, parameters)
        
        if not weather_data:
            return 70  # Default score
        
        # Calculate total rainfall
        total_rainfall = sum(float(item.get('rainfall_1h', 0)) for item in weather_data)
        avg_humidity = sum(float(item.get('humidity', 50)) for item in weather_data) / len(weather_data)
        
        # Water efficiency logic
        if total_rainfall < 10:  # Low rainfall period
            if avg_humidity > 60:
                efficiency_score = 85  # Good humidity retention
            else:
                efficiency_score = 60
        else:  # Good rainfall period
            efficiency_score = 80  # Adequate water
        
        return round(efficiency_score, 1)
    
    def _calculate_air_quality_score(self, farm_id: str):
        """Calculate air quality score"""
        
        query = """
        SELECT * FROM c 
        WHERE c.farm_id = @farm_id AND c.sensor_type = 'air_quality'
        AND c.timestamp >= @time_window
        ORDER BY c.timestamp DESC
        """
        
        time_window = (datetime.utcnow() - timedelta(days=7)).isoformat()
        parameters = [
            {"name": "@farm_id", "value": farm_id},
            {"name": "@time_window", "value": time_window}
        ]
        
        air_data = self.db.query_items('sensor_data', query, parameters)
        
        if not air_data:
            return 75  # Default score
        
        # Calculate averages
        avg_co2 = sum(float(item.get('co2', 400)) for item in air_data) / len(air_data)
        avg_ch4 = sum(float(item.get('ch4', 0)) for item in air_data) / len(air_data)
        
        # Score based on air quality thresholds
        co2_score = max(0, 100 - (avg_co2 - 400) * 0.1)
        ch4_score = max(0, 100 - avg_ch4 * 10)
        
        air_score = (co2_score + ch4_score) / 2
        
        return round(min(air_score, 100), 1)
    
    def _calculate_crop_health_score(self, farm_id: str):
        """Calculate crop health score"""
        
        # Use soil health as proxy for crop health
        soil_score = self._calculate_soil_health_score(farm_id)
        crop_score = soil_score * 0.9  # Slightly lower than soil health
        
        return round(crop_score, 1)
    
    def _calculate_risk_management_score(self, farm_id: str):
        """Calculate risk management score"""
        
        # Get recent fire alerts
        query = """
        SELECT TOP 1 * FROM c 
        WHERE c.farm_id = @farm_id 
        ORDER BY c.timestamp DESC
        """
        
        parameters = [{"name": "@farm_id", "value": farm_id}]
        fire_alerts = self.db.query_items('fire_alerts', query, parameters)
        
        risk_score = 100  # Start with perfect score
        
        if fire_alerts:
            fire_risk = fire_alerts[0].get('risk_level', 'low')
            if fire_risk == 'critical':
                risk_score -= 40
            elif fire_risk == 'high':
                risk_score -= 25
            elif fire_risk == 'moderate':
                risk_score -= 10
        
        return max(0, risk_score)
    
    def _score_ph_level(self, ph: float):
        """Score pH level (optimal range 6.0-7.5)"""
        if 6.0 <= ph <= 7.5:
            return 100
        elif 5.5 <= ph < 6.0 or 7.5 < ph <= 8.0:
            return 80
        else:
            return 60
    
    def _score_moisture_level(self, moisture: float):
        """Score soil moisture level"""
        if 40 <= moisture <= 70:
            return 100
        elif 30 <= moisture < 40 or 70 < moisture <= 80:
            return 80
        else:
            return 60
    
    def _score_nitrogen_level(self, nitrogen: float):
        """Score nitrogen levels"""
        if 50 <= nitrogen <= 100:
            return 100
        elif 30 <= nitrogen < 50:
            return 80
        else:
            return 60
    
    def _determine_grade(self, score: int):
        """Determine letter grade from score"""
        if score >= 900:
            return 'A+'
        elif score >= 850:
            return 'A'
        elif score >= 800:
            return 'A-'
        elif score >= 750:
            return 'B+'
        elif score >= 700:
            return 'B'
        elif score >= 650:
            return 'B-'
        elif score >= 600:
            return 'C+'
        else:
            return 'C'

def get_active_farms():
    """Get list of active farms"""
    return [
        {'id': 'FARM_001', 'name': 'BotanicaX Demo Farm 1'},
        {'id': 'FARM_002', 'name': 'BotanicaX Demo Farm 2'}
    ]
