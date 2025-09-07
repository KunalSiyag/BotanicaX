# scripts/view_data.py
import sqlite3
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATABASE_PATH, FARMS

def view_latest_data(farm_id=None, hours=24):
    """View recent data from database"""
    
    if not os.path.exists(DATABASE_PATH):
        print("❌ Database not found! Run setup_database.py first.")
        return
    
    conn = sqlite3.connect(DATABASE_PATH)
    
    print(f"📊 SMART AGRICHAIN DATA VIEWER")
    print(f"🕒 Last {hours} hours")
    print("=" * 60)
    
    if farm_id:
        farm_filter = f"WHERE farm_id = '{farm_id}'"
        print(f"🏠 Farm: {farm_id}")
    else:
        farm_filter = ""
        print("🏠 All farms")
    
    print()
    
    # Weather data
    weather_query = f'''
    SELECT farm_id, timestamp, temperature, humidity, rainfall_1h, weather_condition
    FROM weather_data 
    {farm_filter}
    {"AND" if farm_filter else "WHERE"} datetime(timestamp) >= datetime('now', '-{hours} hours')
    ORDER BY timestamp DESC LIMIT 5
    '''
    
    weather_df = pd.read_sql_query(weather_query, conn)
    
    if not weather_df.empty:
        print("🌤️  RECENT WEATHER DATA:")
        print("-" * 40)
        for _, row in weather_df.iterrows():
            print(f"   {row['farm_id']} | {row['timestamp'][:19]}")
            print(f"   🌡️  {row['temperature']}°C | 💧 {row['humidity']}% | 🌧️  {row['rainfall_1h']}mm")
            print(f"   Weather: {row['weather_condition']}")
            print()
    else:
        print("🌤️  No weather data found")
    
    # Sensor data
    sensor_query = f'''
    SELECT farm_id, timestamp, soil_moisture, soil_temperature, nitrogen, phosphorus, potassium, growth_stage
    FROM soil_sensor_data 
    {farm_filter}
    {"AND" if farm_filter else "WHERE"} datetime(timestamp) >= datetime('now', '-{hours} hours')
    ORDER BY timestamp DESC LIMIT 5
    '''
    
    sensor_df = pd.read_sql_query(sensor_query, conn)
    
    if not sensor_df.empty:
        print("🌱 RECENT SENSOR DATA:")
        print("-" * 40)
        for _, row in sensor_df.iterrows():
            print(f"   {row['farm_id']} | {row['timestamp'][:19]}")
            print(f"   💧 Soil Moisture: {row['soil_moisture']}%")
            print(f"   🌡️  Soil Temperature: {row['soil_temperature']}°C")
            print(f"   🌿 NPK: N={row['nitrogen']}, P={row['phosphorus']}, K={row['potassium']}")
            print(f"   🌱 Stage: {row['growth_stage']}")
            print()
    else:
        print("🌱 No sensor data found")
    
    # Fire alerts
    fire_query = f'''
    SELECT farm_id, timestamp, risk_level, nearby_fires, recommendation
    FROM fire_alerts 
    {farm_filter}
    {"AND" if farm_filter else "WHERE"} datetime(timestamp) >= datetime('now', '-{hours} hours')
    ORDER BY timestamp DESC LIMIT 3
    '''
    
    fire_df = pd.read_sql_query(fire_query, conn)
    
    if not fire_df.empty:
        print("🔥 FIRE ALERTS:")
        print("-" * 40)
        for _, row in fire_df.iterrows():
            risk_emoji = "🚨" if row['risk_level'] == 'critical' else "⚠️" if row['risk_level'] == 'high' else "🟡"
            print(f"   {row['farm_id']} | {row['timestamp'][:19]}")
            print(f"   {risk_emoji} Risk: {row['risk_level'].upper()}")
            print(f"   🔥 Nearby fires: {row['nearby_fires']}")
            print(f"   💡 {row['recommendation']}")
            print()
    else:
        print("🔥 No fire alerts")
    
    # Sustainability scores
    score_query = f'''
    SELECT farm_id, timestamp, overall_score, grade, recommendations
    FROM sustainability_scores 
    {farm_filter}
    {"AND" if farm_filter else "WHERE"} datetime(timestamp) >= datetime('now', '-{hours} hours')
    ORDER BY timestamp DESC LIMIT 3
    '''
    
    score_df = pd.read_sql_query(score_query, conn)
    
    if not score_df.empty:
        print("🌱 SUSTAINABILITY SCORES:")
        print("-" * 40)
        for _, row in score_df.iterrows():
            score = row['overall_score']
            emoji = "🌟" if score >= 800 else "💚" if score >= 700 else "🟡" if score >= 600 else "🔴"
            print(f"   {row['farm_id']} | {row['timestamp'][:19]}")
            print(f"   {emoji} Score: {score}/1000 ({row['grade']})")
            
            # Show recommendations
            if row['recommendations']:
                import json
                try:
                    recs = json.loads(row['recommendations'])
                    if recs:
                        print(f"   💡 {recs[0]}")
                except:
                    print(f"   💡 {row['recommendations']}")
            print()
    else:
        print("🌱 No sustainability scores")
    
    conn.close()

def show_farm_summary():
    """Show summary for all farms"""
    conn = sqlite3.connect(DATABASE_PATH)
    
    print("🏠 FARM SUMMARY")
    print("=" * 50)
    
    for farm in FARMS:
        farm_id = farm['id']
        farm_name = farm['name']
        
        print(f"\n📍 {farm_name} ({farm_id})")
        print("-" * 30)
        
        # Count records
        weather_count = pd.read_sql_query(
            f"SELECT COUNT(*) as count FROM weather_data WHERE farm_id = '{farm_id}'", 
            conn
        ).iloc[0]['count']
        
        sensor_count = pd.read_sql_query(
            f"SELECT COUNT(*) as count FROM soil_sensor_data WHERE farm_id = '{farm_id}'", 
            conn
        ).iloc[0]['count']
        
        print(f"   📊 Weather records: {weather_count}")
        print(f"   📊 Sensor records: {sensor_count}")
        
        # Latest sustainability score
        latest_score = pd.read_sql_query(
            f"SELECT overall_score, grade FROM sustainability_scores WHERE farm_id = '{farm_id}' ORDER BY timestamp DESC LIMIT 1", 
            conn
        )
        
        if not latest_score.empty:
            score = latest_score.iloc[0]['overall_score']
            grade = latest_score.iloc[0]['grade']
            emoji = "🌟" if score >= 800 else "💚" if score >= 700 else "🟡"
            print(f"   {emoji} Current Score: {score}/1000 ({grade})")
        else:
            print("   🌱 No sustainability score yet")
    
    conn.close()

def main():
    print("🔍 Smart AgriChain Data Viewer")
    print("What would you like to view?")
    print("1. Latest data (last 24 hours)")
    print("2. Latest data (last 7 days)")
    print("3. Farm summary")
    print("4. Specific farm data")
    
    choice = input("\nEnter choice (1-4): ").strip()
    
    if choice == '1':
        view_latest_data(hours=24)
    elif choice == '2':
        view_latest_data(hours=24*7)
    elif choice == '3':
        show_farm_summary()
    elif choice == '4':
        print("\nAvailable farms:")
        for i, farm in enumerate(FARMS, 1):
            print(f"   {i}. {farm['name']} ({farm['id']})")
        
        try:
            farm_choice = int(input("\nEnter farm number: ")) - 1
            if 0 <= farm_choice < len(FARMS):
                farm_id = FARMS[farm_choice]['id']
                view_latest_data(farm_id=farm_id)
            else:
                print("❌ Invalid farm number")
        except ValueError:
            print("❌ Please enter a valid number")
    else:
        print("❌ Invalid choice")

if __name__ == "__main__":
    main()
