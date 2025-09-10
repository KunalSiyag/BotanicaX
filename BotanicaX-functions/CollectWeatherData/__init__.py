import azure.functions as func
import logging
import requests
import os
import sys
from datetime import datetime

# Add shared_code to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared_code'))
from database_helper import CosmosDBHelper

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info(f'BotanicaX Weather Collection executed at: {utc_timestamp}')
    
    try:
        # Initialize database helper
        db = CosmosDBHelper()
        
        # Get farm locations
        farm_locations = get_farm_locations()
        
        # Collect weather data for each farm
        weather_collector = WeatherDataCollector()
        nasa_collector = NASADataCollector()
        
        for farm in farm_locations:
            try:
                # Collect OpenWeather data
                weather_data = weather_collector.fetch_weather_data(
                    farm['id'], 
                    farm['latitude'], 
                    farm['longitude']
                )
                
                if weather_data:
                    db.insert_item('weather_data', weather_data)
                    logging.info(f"Weather data collected for farm {farm['id']}")
                
                # Collect NASA FIRMS fire data (less frequently)
                if datetime.utcnow().hour % 6 == 0:  # Every 6 hours
                    fire_data = nasa_collector.fetch_fire_data(
                        farm['id'],
                        farm['latitude'],
                        farm['longitude']
                    )
                    
                    if fire_data:
                        db.insert_item('fire_alerts', fire_data)
                        logging.info(f"Fire data collected for farm {farm['id']}")
                
            except Exception as e:
                logging.error(f"Error collecting data for farm {farm['id']}: {e}")
        
        logging.info("Weather collection completed successfully")
        
    except Exception as e:
        logging.error(f"Error in weather collection: {e}")
        raise

class WeatherDataCollector:
    def __init__(self):
        self.api_key = os.environ.get('OPENWEATHER_API_KEY')
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
    
    def fetch_weather_data(self, farm_id: str, lat: float, lon: float):
        """Fetch weather data from OpenWeatherMap API"""
        
        if not self.api_key:
            logging.error("OpenWeatherMap API key not configured")
            return None
        
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Transform to BotanicaX format
            weather_data = {
                'farm_id': farm_id,
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'wind_speed': data.get('wind', {}).get('speed', 0),
                'rainfall_1h': data.get('rain', {}).get('1h', 0),
                'weather_condition': data['weather'][0]['main'],
                'weather_description': data['weather'][0]['description'],
                'visibility': data.get('visibility', 10000),
                'cloudiness': data['clouds']['all'],
                'data_source': 'openweathermap',
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return weather_data
            
        except requests.RequestException as e:
            logging.error(f"API request failed: {e}")
            return None
        except KeyError as e:
            logging.error(f"Unexpected API response format: {e}")
            return None

class NASADataCollector:
    def __init__(self):
        self.api_key = os.environ.get('NASA_FIRMS_API_KEY')
        self.base_url = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
    
    def fetch_fire_data(self, farm_id: str, lat: float, lon: float, radius_km: int = 20):
        """Fetch fire data from NASA FIRMS API"""
        
        if not self.api_key:
            logging.error("NASA FIRMS API key not configured")
            return None
        
        url = f"{self.base_url}/{self.api_key}/VIIRS_NOAA20_NRT/{lat},{lon},{radius_km}/1"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            csv_data = response.text.strip()
            
            if csv_data and len(csv_data.split('\n')) > 1:
                lines = csv_data.split('\n')
                fire_count = len(lines) - 1  # Subtract header
                
                # Analyze fire risk
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
                    'radius_km': radius_km,
                    'recommendation': get_fire_recommendation(risk_level),
                    'data_source': 'nasa_firms',
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                return fire_alert
            else:
                # No fires detected
                return {
                    'farm_id': farm_id,
                    'risk_level': 'low',
                    'nearby_fires': 0,
                    'radius_km': radius_km,
                    'recommendation': 'Continue normal operations',
                    'data_source': 'nasa_firms',
                    'timestamp': datetime.utcnow().isoformat()
                }
                
        except requests.RequestException as e:
            logging.error(f"NASA API request failed: {e}")
            return None

def get_fire_recommendation(risk_level: str) -> str:
    """Get recommendation based on fire risk level"""
    recommendations = {
        'low': 'Continue normal operations',
        'moderate': 'Monitor situation closely',
        'high': 'Prepare for potential evacuation',
        'critical': 'Immediate evacuation may be required'
    }
    return recommendations.get(risk_level, 'Monitor situation')

def get_farm_locations():
    """Get farm locations - in production, fetch from Cosmos DB"""
    return [
        {
            'id': 'FARM_001',
            'name': 'BotanicaX Demo Farm Visakhapatnam',
            'latitude': 17.6868,
            'longitude': 83.2185
        },
        {
            'id': 'FARM_002', 
            'name': 'BotanicaX Demo Farm Delhi',
            'latitude': 28.7041,
            'longitude': 77.1025
        }
    ]
