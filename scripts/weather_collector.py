# scripts/weather_collector.py
import requests
import sqlite3
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import API_KEYS, DATABASE_PATH

class WeatherCollector:
    def __init__(self):
        self.api_key = API_KEYS['openweather']
        self.base_url = "http://api.openweathermap.org/data/2.5/weather"
        print("🌤️  Weather Collector initialized!")
    
    def fetch_weather_data(self, farm_id, lat, lon):
        """Fetch current weather data for farm location"""
        print(f"🔍 Fetching weather for farm {farm_id}...")
        
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                
                weather_data = {
                    'farm_id': farm_id,
                    'timestamp': datetime.now().isoformat(),
                    'temperature': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'pressure': data['main']['pressure'],
                    'wind_speed': data.get('wind', {}).get('speed', 0),
                    'rainfall_1h': data.get('rain', {}).get('1h', 0),
                    'weather_condition': data['weather'][0]['main']
                }
                
                print(f"✅ Weather data fetched successfully!")
                print(f"   🌡️  Temperature: {weather_data['temperature']}°C")
                print(f"   💧 Humidity: {weather_data['humidity']}%")
                print(f"   🌧️  Rainfall: {weather_data['rainfall_1h']}mm")
                
                return weather_data
                
            else:
                print(f"❌ API Error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching weather data: {e}")
            return None
    
    def save_to_database(self, weather_data):
        """Save weather data to database"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO weather_data 
            (farm_id, timestamp, temperature, humidity, pressure, wind_speed, rainfall_1h, weather_condition)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                weather_data['farm_id'], weather_data['timestamp'],
                weather_data['temperature'], weather_data['humidity'],
                weather_data['pressure'], weather_data['wind_speed'],
                weather_data['rainfall_1h'], weather_data['weather_condition']
            ))
            
            conn.commit()
            conn.close()
            
            print("💾 Weather data saved to database!")
            return True
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False

# Test function
def test_weather_collector():
    collector = WeatherCollector()
    
    # Test with sample coordinates
    weather_data = collector.fetch_weather_data('FARM001', 17.6868, 83.2185)
    
    if weather_data:
        success = collector.save_to_database(weather_data)
        if success:
            print("🎉 Weather collection test successful!")
        else:
            print("⚠️  Data fetched but failed to save.")
    else:
        print("❌ Weather collection test failed!")

if __name__ == "__main__":
    test_weather_collector()
