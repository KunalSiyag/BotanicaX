# scripts/fire_detector.py
import requests
import sqlite3
import math
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import API_KEYS, DATABASE_PATH

class FireDetector:
    def __init__(self):
        self.nasa_map_key = API_KEYS.get('nasa_firms', 'YOUR_NASA_MAP_KEY')
        self.base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
        print("🔥 Fire Detection System initialized!")
    
    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points"""
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        distance = math.sqrt(dlat**2 + dlon**2) * 111  # Rough km conversion
        return distance
    
    def fetch_fire_data(self, farm_id, lat, lon, radius_km=20, days=7):
        """Fetch fire data from NASA FIRMS"""
        print(f"🔍 Checking for fires around {farm_id}...")
        
        url = f"{self.base_url}/{self.nasa_map_key}/VIIRS_NOAA20_NRT/{lat},{lon},{radius_km}/{days}"
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                csv_data = response.text.strip()
                
                if csv_data and len(csv_data.split('\n')) > 1:
                    lines = csv_data.split('\n')
                    headers = lines[0].split(',')
                    
                    fire_incidents = []
                    for line in lines[1:]:
                        if line.strip():
                            values = line.split(',')
                            if len(values) >= len(headers):
                                fire_data = dict(zip(headers, values))
                                fire_incidents.append(fire_data)
                    
                    print(f"🔥 Found {len(fire_incidents)} fire incidents")
                    return fire_incidents
                else:
                    print("✅ No fires detected")
                    return []
            else:
                print(f"❌ NASA API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching fire data: {e}")
            return None
    
    def analyze_fire_risk(self, farm_id, lat, lon, fire_incidents):
        """Analyze fire risk for the farm"""
        if not fire_incidents:
            return {
                'farm_id': farm_id,
                'risk_level': 'low',
                'nearby_fires': 0,
                'closest_fire_km': None,
                'recommendation': 'Continue normal operations'
            }
        
        nearby_fires = 0
        high_confidence_fires = 0
        closest_distance = float('inf')
        
        for fire in fire_incidents:
            try:
                fire_lat = float(fire.get('latitude', 0))
                fire_lon = float(fire.get('longitude', 0))
                confidence = int(fire.get('confidence', 0))
                
                distance = self.calculate_distance(lat, lon, fire_lat, fire_lon)
                
                if distance < 50:
                    nearby_fires += 1
                    closest_distance = min(closest_distance, distance)
                    
                    if confidence > 75:
                        high_confidence_fires += 1
                        
            except (ValueError, TypeError):
                continue
        
        # Determine risk level
        if high_confidence_fires > 2 or closest_distance < 5:
            risk_level = 'critical'
            recommendation = 'Immediate evacuation may be required'
        elif high_confidence_fires > 0 or closest_distance < 15:
            risk_level = 'high'
            recommendation = 'Prepare for potential evacuation'
        elif nearby_fires > 0:
            risk_level = 'moderate'
            recommendation = 'Monitor situation closely'
        else:
            risk_level = 'low'
            recommendation = 'Continue normal operations'
        
        return {
            'farm_id': farm_id,
            'risk_level': risk_level,
            'nearby_fires': nearby_fires,
            'closest_fire_km': round(closest_distance, 1) if closest_distance != float('inf') else None,
            'recommendation': recommendation
        }
    
    def save_fire_analysis(self, fire_analysis):
        """Save fire analysis to database"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO fire_alerts 
            (farm_id, timestamp, risk_level, nearby_fires, closest_fire_km, recommendation)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                fire_analysis['farm_id'], datetime.now().isoformat(),
                fire_analysis['risk_level'], fire_analysis['nearby_fires'],
                fire_analysis['closest_fire_km'], fire_analysis['recommendation']
            ))
            
            conn.commit()
            conn.close()
            
            print("💾 Fire analysis saved!")
            return True
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False

def test_fire_detector():
    detector = FireDetector()
    
    farm_id = "FARM001"
    lat, lon = 17.6868, 83.2185
    
    fire_incidents = detector.fetch_fire_data(farm_id, lat, lon)
    
    if fire_incidents is not None:
        fire_analysis = detector.analyze_fire_risk(farm_id, lat, lon, fire_incidents)
        
        print("\n🔥 FIRE RISK ANALYSIS:")
        print(f"   🏠 Farm: {fire_analysis['farm_id']}")
        print(f"   ⚠️  Risk Level: {fire_analysis['risk_level'].upper()}")
        print(f"   🔥 Nearby Fires: {fire_analysis['nearby_fires']}")
        if fire_analysis['closest_fire_km']:
            print(f"   📍 Closest Fire: {fire_analysis['closest_fire_km']} km")
        print(f"   💡 Recommendation: {fire_analysis['recommendation']}")
        
        detector.save_fire_analysis(fire_analysis)

if __name__ == "__main__":
    test_fire_detector()
