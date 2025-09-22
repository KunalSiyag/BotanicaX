import sys
import os
import json
import uuid
import requests
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import random
import csv
from io import StringIO

# Add shared code path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'BotanicaX-functions', 'shared_code'))

def load_local_settings():
    settings_path = Path(__file__).parent.parent / 'BotanicaX-functions' / 'local.settings.json'
    if settings_path.exists():
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        for k, v in settings.get('Values', {}).items():
            os.environ[k] = v
        print(f"✅ Loaded settings from {settings_path}")
    else:
        print(f"❌ Settings file not found at {settings_path}")

load_local_settings()

from database_helper import CosmosDBHelper

def generate_fake_soil_data(db: CosmosDBHelper, farm_id: str, days=7):
    print(f"Generating {days} days of synthetic soil sensor data for {farm_id}")
    for day in range(days):
        base_time = datetime.utcnow() - timedelta(days=day)
        for hour in [6, 12, 18, 24]:
            timestamp = base_time.replace(hour=hour % 24, minute=0, second=0).isoformat()
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
                'timestamp': timestamp,
                'id': f"{farm_id}_soil_{timestamp}_{uuid.uuid4()}"
            }
            try:
                db.insert_item('sensor_data', soil_data)
            except Exception as e:
                logging.error(f"Error inserting soil data: {e}")
    print(f"Completed generating synthetic soil sensor data for {farm_id}")

def get_bounding_box(lat, lon, delta=0.15):
    west = lon - delta
    south = lat - delta
    east = lon + delta
    north = lat + delta
    return f"{west},{south},{east},{north}"

def get_fire_recommendation(risk_level: str) -> str:
    recommendations = {
        'low': 'Continue normal operations',
        'moderate': 'Monitor situation closely',
        'high': 'Prepare for potential evacuation',
        'critical': 'Immediate evacuation may be required'
    }
    return recommendations.get(risk_level, 'Monitor situation')

def fetch_and_insert_fire_alerts(db: CosmosDBHelper, farm):
    farm_id = farm['id']
    lat = farm['latitude']
    lon = farm['longitude']    
    api_key = os.environ.get('NASA_FIRMS_API_KEY')
    if not api_key:
        logging.error("NASA_FIRMS_API_KEY environment variable not set")
        return
    
    bbox = get_bounding_box(lat, lon)
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
    url = f"{base_url}/{api_key}/VIIRS_NOAA20_NRT/{bbox}/1"
    
    print(f"Fetching fire alerts from NASA FIRMS API for farm {farm_id}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        csv_data = response.text.strip()
        if csv_data and len(csv_data.split('\n')) > 1:
            csv_file = StringIO(csv_data)
            reader = csv.DictReader(csv_file)
            fire_hotspots = list(reader)
            fire_count = len(fire_hotspots)
            
            risk_level = 'low'
            if fire_count > 10:
                risk_level = 'critical'
            elif fire_count > 5:
                risk_level = 'high'
            elif fire_count > 0:
                risk_level = 'moderate'
            
            fire_alert = {
                'farm_id': farm_id,
                'risk_level': risk_level,
                'nearby_fires': fire_count,
                'radius_km': 20,
                'recommendation': get_fire_recommendation(risk_level),
                'data_source': 'nasa_firms',
                'timestamp': datetime.utcnow().isoformat(),
                'id': f"{farm_id}_fire_{uuid.uuid4()}"
            }
            db.insert_item('fire_alerts', fire_alert)
            print(f"Inserted fire alert data for {farm_id} with risk level {risk_level}")
        else:
            print(f"No fire alerts found for farm {farm_id}")
    except Exception as e:
        logging.error(f"NASA FIRMS API request failed for farm {farm_id}: {e}")

def fetch_and_insert_openweather_data(db: CosmosDBHelper, farm):
    farm_id = farm['id']
    lat = farm['latitude']
    lon = farm['longitude']
    openweather_api_url = "http://api.openweathermap.org/data/2.5/weather"
    api_key = os.environ.get('OPENWEATHER_API_KEY')
    if not api_key:
        logging.error("OPENWEATHER_API_KEY environment variable not set")
        return
    
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': 'metric'
    }
    
    print(f"Fetching OpenWeather data for farm {farm_id}")
    
    try:
        response = requests.get(openweather_api_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        weather_doc = {
            'farm_id': farm_id,
            'sensor_type': 'weather_station',
            'temperature': data.get('main', {}).get('temp'),
            'humidity': data.get('main', {}).get('humidity'),
            'pressure': data.get('main', {}).get('pressure'),
            'wind_speed': data.get('wind', {}).get('speed'),
            'rainfall': data.get('rain', {}).get('1h', 0) if 'rain' in data else 0,
            'weather_condition': data.get('weather', [{}])[0].get('main', ''),
            'weather_description': data.get('weather', [{}])[0].get('description', ''),
            'visibility': data.get('visibility', 10000),
            'cloudiness': data.get('clouds', {}).get('all'),
            'device_id': 'openweathermap_api',
            'data_source': 'openweathermap',
            'timestamp': datetime.utcnow().isoformat(),
            'id': f"{farm_id}_openweather_{uuid.uuid4()}"
        }
        db.insert_item('weather_data', weather_doc)
        print(f"Inserted OpenWeather data for farm {farm_id}")
    except Exception as e:
        logging.error(f"Error fetching OpenWeather data for farm {farm_id}: {e}")

def get_farm_locations():
    return [
        {
            'id': 'FARM_001',
            'name': 'BotanicaX Demo Farm Visakhapatnam',
            'latitude': 17.6,
            'longitude': 83.2
        },
        {
            'id': 'FARM_002',
            'name': 'BotanicaX Demo Farm Delhi',
            'latitude': 28.8,
            'longitude': 77.4
        }
    ]

def run_data_generation(db, farms):
    for farm in farms:
        farm_id = farm['id']
        print(f"\nProcessing farm {farm_id}")
        generate_fake_soil_data(db, farm_id, days=14)
        fetch_and_insert_fire_alerts(db, farm)
        fetch_and_insert_openweather_data(db, farm)
        print(f"Completed data generation for farm {farm_id}")
    print("All data generation done!")

def main():
    while True:
        choice = input("Run mode? Enter 'once' for single run, 'continuous' for continuous every 30 minutes: ").strip().lower()
        if choice in ['once', 'continuous']:
            break
        print("Invalid input. Please enter 'once' or 'continuous'.")

    print("Starting BotanicaX data generation script")

    try:
        db = CosmosDBHelper()
        farms = get_farm_locations()

        if choice == 'once':
            run_data_generation(db, farms)
            print("Finished single run.")
        else:
            print("Running continuous mode every 30 minutes. Press Ctrl+C to stop.")
            try:
                while True:
                    run_data_generation(db, farms)
                    print("Sleeping for 30 minutes...")
                    time.sleep(1800)
            except KeyboardInterrupt:
                print("Continuous execution stopped by user.")
    except Exception as e:
        logging.error(f"Error in main loop: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
