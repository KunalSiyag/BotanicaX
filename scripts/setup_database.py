# scripts/setup_database.py
import sqlite3
import os
from datetime import datetime

def create_database():
    """Create all database tables for Smart AgriChain"""
    
    # Ensure database directory exists
    db_path = 'database/farm_data.db'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🗄️  Creating Smart AgriChain database...")
    
    # Weather data table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        farm_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        temperature REAL,
        humidity REAL,
        pressure REAL,
        wind_speed REAL,
        rainfall_1h REAL,
        weather_condition TEXT,
        collected_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Soil sensor data table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS soil_sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        farm_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        soil_moisture REAL,
        soil_temperature REAL,
        soil_ph REAL,
        nitrogen REAL,
        phosphorus REAL,
        potassium REAL,
        growth_stage TEXT,
        crop_day INTEGER,
        collected_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Fire alerts table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fire_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        farm_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        risk_level TEXT,
        nearby_fires INTEGER,
        closest_fire_km REAL,
        recommendation TEXT,
        collected_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Sustainability scores table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sustainability_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        farm_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        overall_score INTEGER,
        grade TEXT,
        soil_health_score REAL,
        water_efficiency_score REAL,
        crop_health_score REAL,
        risk_management_score REAL,
        recommendations TEXT,
        collected_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()
    
    print("✅ Database created successfully!")
    print("📍 Location: database/farm_data.db")

if __name__ == "__main__":
    create_database()
