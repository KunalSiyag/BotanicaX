# File: scripts/generate_fake_data.py
import sys
import os
import json
from pathlib import Path

# Add function path for shared code
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'BotanicaX-functions', 'shared_code'))

def load_local_settings():
    """Load environment variables from local.settings.json"""
    settings_path = Path(__file__).parent.parent / 'BotanicaX-functions' / 'local.settings.json'
    
    if settings_path.exists():
        with open(settings_path, 'r') as f:
            settings = json.load(f)
            
        # Set environment variables
        for key, value in settings.get('Values', {}).items():
            os.environ[key] = value
            
        print(f"✅ Loaded settings from {settings_path}")
    else:
        print(f"❌ Settings file not found at {settings_path}")

# Load settings before importing database helper
load_local_settings()

from database_helper import CosmosDBHelper
import random
from datetime import datetime, timedelta
import json

def generate_fake_sensor_data(db: CosmosDBHelper, farm_id: str, days: int = 7):
    """Generate fake sensor data for testing"""
    
    print(f"Generating {days} days of fake sensor data for {farm_id}")
    
    for day in range(days):
        # Generate data for each day
        base_time = datetime.utcnow() - timedelta(days=day)
        
        # Generate 4 readings per day (every 6 hours)
        for hour in [6, 12, 18, 24]:
            timestamp = base_time.replace(hour=hour % 24, minute=0, second=0).isoformat()
            
            # Soil sensor data
            soil_data = {
                'farm_id': farm_id,
                'sensor_type': 'soil',
                'soil_moisture': round(random.uniform(30, 80), 1),
                'soil_temperature': round(random.uniform(18, 35), 1),
                'soil_ph': round(random.uniform(6.0, 7.8), 1),
                'nitrogen': round(random.uniform(40, 120), 1),
                'phosphorus': round(random.uniform(15, 60), 1),
                'potassium': round(random.uniform(80, 200), 1),
                'organic_carbon': round(random.uniform(1.5, 4.0), 1),
                'device_id': 'SOIL_SENSOR_001',
                'battery_level': random.randint(70, 100),
                'data_source': 'simulation',
                'timestamp': timestamp
            }
            
            # Air quality sensor data
            air_data = {
                'farm_id': farm_id,
                'sensor_type': 'air_quality',
                'co2': round(random.uniform(380, 450), 1),
                'ch4': round(random.uniform(0.1, 2.0), 2),
                'n2o': round(random.uniform(0.1, 1.0), 2),
                'pm25': round(random.uniform(5, 35), 1),
                'device_id': 'AIR_SENSOR_001',
                'data_source': 'simulation',
                'timestamp': timestamp
            }
            
            # Weather station data (if available)
            weather_station_data = {
                'farm_id': farm_id,
                'sensor_type': 'weather_station',
                'temperature': round(random.uniform(20, 40), 1),
                'humidity': round(random.uniform(40, 90), 1),
                'pressure': round(random.uniform(990, 1030), 1),
                'wind_speed': round(random.uniform(0, 25), 1),
                'rainfall': round(random.uniform(0, 15), 1),
                'device_id': 'WEATHER_STATION_001',
                'data_source': 'simulation',
                'timestamp': timestamp
            }
            
            try:
                # Insert into Cosmos DB
                db.insert_item('sensor_data', soil_data)
                db.insert_item('sensor_data', air_data)
                db.insert_item('sensor_data', weather_station_data)
                
            except Exception as e:
                print(f"Error inserting data: {e}")
    
    print(f"Completed generating fake sensor data for {farm_id}")

def generate_fake_weather_data(db: CosmosDBHelper, farm_id: str, days: int = 7):
    """Generate fake weather data"""
    
    print(f"Generating {days} days of fake weather data for {farm_id}")
    
    for day in range(days):
        # Generate weather data every 3 hours
        base_time = datetime.utcnow() - timedelta(days=day)
        
        for hour in range(0, 24, 3):
            timestamp = base_time.replace(hour=hour, minute=0, second=0).isoformat()
            
            weather_data = {
                'farm_id': farm_id,
                'temperature': round(random.uniform(15, 40), 1),
                'humidity': round(random.uniform(30, 95), 1),
                'pressure': round(random.uniform(995, 1025), 1),
                'wind_speed': round(random.uniform(0, 30), 1),
                'rainfall_1h': round(random.uniform(0, 10), 1),
                'weather_condition': random.choice(['Clear', 'Clouds', 'Rain', 'Partly Cloudy']),
                'visibility': random.randint(5000, 10000),
                'cloudiness': random.randint(0, 100),
                'data_source': 'simulation',
                'timestamp': timestamp
            }
            
            try:
                db.insert_item('weather_data', weather_data)
            except Exception as e:
                print(f"Error inserting weather data: {e}")
    
    print(f"Completed generating fake weather data for {farm_id}")

def generate_fake_fire_alerts(db: CosmosDBHelper, farm_id: str, count: int = 5):
    """Generate fake fire alerts"""
    
    print(f"Generating {count} fake fire alerts for {farm_id}")
    
    for i in range(count):
        days_ago = random.randint(0, 30)
        timestamp = (datetime.utcnow() - timedelta(days=days_ago)).isoformat()
        
        risk_level = random.choice(['low', 'low', 'moderate', 'high', 'critical'])  # Bias toward low
        nearby_fires = 0
        
        if risk_level == 'critical':
            nearby_fires = random.randint(10, 20)
        elif risk_level == 'high':
            nearby_fires = random.randint(5, 10)
        elif risk_level == 'moderate':
            nearby_fires = random.randint(1, 5)
        
        fire_alert = {
            'farm_id': farm_id,
            'risk_level': risk_level,
            'nearby_fires': nearby_fires,
            'radius_km': 20,
            'recommendation': get_fire_recommendation(risk_level),
            'data_source': 'simulation',
            'timestamp': timestamp
        }
        
        try:
            db.insert_item('fire_alerts', fire_alert)
        except Exception as e:
            print(f"Error inserting fire alert: {e}")
    
    print(f"Completed generating fake fire alerts for {farm_id}")

def generate_fake_sustainability_scores(db: CosmosDBHelper, farm_id: str, count: int = 30):
    """Generate fake sustainability scores (daily for last 30 days)"""
    
    print(f"Generating {count} fake sustainability scores for {farm_id}")
    
    base_score = random.randint(650, 850)  # Starting score
    
    for i in range(count):
        days_ago = count - i - 1
        timestamp = (datetime.utcnow() - timedelta(days=days_ago)).replace(hour=2, minute=0, second=0).isoformat()
        
        # Add some variation to the score
        score_variation = random.randint(-30, 30)
        current_score = max(400, min(1000, base_score + score_variation))
        
        # Determine grade based on score
        if current_score >= 900:
            grade = 'A+'
        elif current_score >= 850:
            grade = 'A'
        elif current_score >= 800:
            grade = 'A-'
        elif current_score >= 750:
            grade = 'B+'
        elif current_score >= 700:
            grade = 'B'
        else:
            grade = 'C'
        
        # Generate component scores
        components = {
            'soil_health': round(random.uniform(60, 95), 1),
            'water_efficiency': round(random.uniform(55, 90), 1),
            'air_quality': round(random.uniform(65, 95), 1),
            'crop_health': round(random.uniform(60, 90), 1),
            'risk_management': round(random.uniform(70, 100), 1)
        }
        
        score_data = {
            'farm_id': farm_id,
            'overall_score': current_score,
            'grade': grade,
            'components': components,
            'calculation_method': 'simulation',
            'timestamp': timestamp
        }
        
        try:
            db.insert_item('sustainability_scores', score_data)
        except Exception as e:
            print(f"Error inserting sustainability score: {e}")
        
        # Update base score for next iteration (simulate gradual improvement)
        base_score += random.randint(-5, 10)  # Slight bias toward improvement
    
    print(f"Completed generating fake sustainability scores for {farm_id}")

def get_fire_recommendation(risk_level: str) -> str:
    """Get recommendation based on fire risk level"""
    recommendations = {
        'low': 'Continue normal operations',
        'moderate': 'Monitor situation closely',
        'high': 'Prepare for potential evacuation',
        'critical': 'Immediate evacuation may be required'
    }
    return recommendations.get(risk_level, 'Monitor situation')

def main():
    """Main function to generate all fake data"""
    
    print("Starting BotanicaX fake data generation...")
    
    try:
        # Initialize database connection
        db = CosmosDBHelper()
        
        # List of farms to generate data for
        farms = ['FARM_001', 'FARM_002']
        
        for farm_id in farms:
            print(f"\n--- Generating data for {farm_id} ---")
            
            # Generate different types of data
            generate_fake_sensor_data(db, farm_id, days=14)
            generate_fake_weather_data(db, farm_id, days=14)
            generate_fake_fire_alerts(db, farm_id, count=10)
            generate_fake_sustainability_scores(db, farm_id, count=30)
            
            print(f"Completed all data generation for {farm_id}")
        
        print("\n🎉 Fake data generation completed successfully!")
        print("You can now test your APIs with this generated data.")
        
    except Exception as e:
        print(f"Error generating fake data: {e}")

if __name__ == "__main__":
    main()
