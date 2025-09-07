# scripts/sensor_simulator.py
import random
import math
import sqlite3
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATABASE_PATH

class SensorSimulator:
    def __init__(self, farm_id, farm_profile):
        self.farm_id = farm_id
        self.farm_profile = farm_profile
        self.crop_day = random.randint(30, 80)  # Days since planting
        self.last_fertilizer = random.randint(15, 45)  # Days since fertilizer
        
        print(f"🌱 Sensor Simulator created for {farm_id}")
        print(f"   Crop: {farm_profile['crop']}")
        print(f"   Soil Type: {farm_profile['soil_type']}")
    
    def simulate_soil_data(self, weather_temp=25, rainfall=0):
        """Simulate realistic soil sensor readings"""
        
        # Soil Moisture (influenced by weather)
        base_moisture = 45
        if rainfall > 10:
            base_moisture += min(25, rainfall * 0.8)
        if weather_temp > 30:
            base_moisture -= (weather_temp - 30) * 0.5
        
        soil_moisture = max(10, min(80, base_moisture * random.uniform(0.9, 1.1)))
        
        # Soil Temperature (2-5°C lower than air)
        soil_temp = weather_temp - random.uniform(2, 5)
        if soil_moisture > 60:
            soil_temp -= 1
        
        # Soil pH based on soil type
        ph_base = {
            'sandy': 6.8, 'clay': 7.2, 'loamy': 6.9, 'black_soil': 7.8
        }
        base_ph = ph_base.get(self.farm_profile['soil_type'], 6.9)
        soil_ph = base_ph + random.uniform(-0.3, 0.3)
        
        return {
            'soil_moisture': round(soil_moisture, 1),
            'soil_temperature': round(soil_temp, 1),
            'soil_ph': round(max(6.0, min(8.0, soil_ph)), 1)
        }
    
    def simulate_npk_data(self):
        """Simulate NPK levels based on crop growth stage"""
        
        # Determine growth stage
        if self.crop_day < 20:
            stage = 'seedling'
        elif self.crop_day < 60:
            stage = 'vegetative'
        elif self.crop_day < 100:
            stage = 'flowering'
        else:
            stage = 'maturity'
        
        # Base NPK values by crop and stage
        crop = self.farm_profile['crop']
        
        if crop == 'rice':
            npk_base = {
                'seedling': {'N': 55, 'P': 28, 'K': 130},
                'vegetative': {'N': 95, 'P': 35, 'K': 105},
                'flowering': {'N': 70, 'P': 42, 'K': 155},
                'maturity': {'N': 35, 'P': 18, 'K': 95}
            }
        elif crop == 'wheat':
            npk_base = {
                'seedling': {'N': 45, 'P': 32, 'K': 110},
                'vegetative': {'N': 85, 'P': 38, 'K': 115},
                'flowering': {'N': 65, 'P': 45, 'K': 140},
                'maturity': {'N': 30, 'P': 22, 'K': 85}
            }
        else:
            npk_base = npk_base = {
                'seedling': {'N': 55, 'P': 28, 'K': 130},
                'vegetative': {'N': 95, 'P': 35, 'K': 105},
                'flowering': {'N': 70, 'P': 42, 'K': 155},
                'maturity': {'N': 35, 'P': 18, 'K': 95}
            }
        
        base = npk_base[stage]
        
        # Fertilizer effect
        fertilizer_effect = math.exp(-self.last_fertilizer / 35)
        
        nitrogen = base['N'] * (1 + fertilizer_effect) * random.uniform(0.85, 1.15)
        phosphorus = base['P'] * (1 + fertilizer_effect * 0.6) * random.uniform(0.9, 1.1)
        potassium = base['K'] * (1 + fertilizer_effect * 0.4) * random.uniform(0.8, 1.2)
        
        return {
            'nitrogen': round(max(15, nitrogen), 1),
            'phosphorus': round(max(8, phosphorus), 1),
            'potassium': round(max(40, potassium), 1),
            'growth_stage': stage
        }
    
    def get_complete_reading(self, weather_data=None):
        """Generate complete sensor reading"""
        
        if weather_data:
            temp = weather_data.get('temperature', 25)
            rainfall = weather_data.get('rainfall_1h', 0)
        else:
            temp = 25
            rainfall = 0
        
        soil_data = self.simulate_soil_data(temp, rainfall)
        npk_data = self.simulate_npk_data()
        
        complete_reading = {
            'farm_id': self.farm_id,
            'timestamp': datetime.now().isoformat(),
            'crop_day': self.crop_day,
            **soil_data,
            **npk_data
        }
        
        return complete_reading
    
    def save_to_database(self, sensor_data):
        """Save sensor data to database"""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO soil_sensor_data 
            (farm_id, timestamp, soil_moisture, soil_temperature, soil_ph,
             nitrogen, phosphorus, potassium, growth_stage, crop_day)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                sensor_data['farm_id'], sensor_data['timestamp'],
                sensor_data['soil_moisture'], sensor_data['soil_temperature'],
                sensor_data['soil_ph'], sensor_data['nitrogen'],
                sensor_data['phosphorus'], sensor_data['potassium'],
                sensor_data['growth_stage'], sensor_data['crop_day']
            ))
            
            conn.commit()
            conn.close()
            
            return True
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False
    
    def advance_time(self, days=1):
        """Advance simulation time"""
        self.crop_day += days
        self.last_fertilizer += days

# Test function
def test_sensor_simulator():
    farm_profile = {
        'crop': 'rice',
        'soil_type': 'black_soil',
        'farming_type': 'organic'
    }
    
    simulator = SensorSimulator('FARM001', farm_profile)
    reading = simulator.get_complete_reading()
    
    print("🧪 Sample sensor reading:")
    print(f"   🌡️  Soil Temperature: {reading['soil_temperature']}°C")
    print(f"   💧 Soil Moisture: {reading['soil_moisture']}%")
    print(f"   🧪 pH Level: {reading['soil_ph']}")
    print(f"   🌿 NPK: N={reading['nitrogen']}, P={reading['phosphorus']}, K={reading['potassium']}")
    print(f"   🌱 Growth Stage: {reading['growth_stage']}")
    
    success = simulator.save_to_database(reading)
    if success:
        print("💾 Sensor data saved successfully!")

if __name__ == "__main__":
    test_sensor_simulator()
