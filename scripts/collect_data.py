# scripts/collect_data.py
import time
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import FARMS, COLLECTION_INTERVAL
from scripts.weather_collector import WeatherCollector
from scripts.sensor_simulator import SensorSimulator
from scripts.fire_detector import FireDetector
from scripts.sustainability_scorer import SustainabilityScorer

class DataCollectionManager:
    def __init__(self):
        print("🚀 Starting Smart AgriChain Data Collection System")
        print("=" * 60)
        
        self.weather_collector = WeatherCollector()
        self.fire_detector = FireDetector()
        self.sustainability_scorer = SustainabilityScorer()
        
        # Initialize sensor simulators
        self.sensor_simulators = {}
        for farm in FARMS:
            simulator = SensorSimulator(farm['id'], farm['profile'])
            self.sensor_simulators[farm['id']] = simulator
            
        print(f"✅ Initialized for {len(FARMS)} farms")
        print("=" * 60)
    
    def collect_farm_data(self, farm):
        """Collect all data for one farm"""
        farm_id = farm['id']
        farm_name = farm['name']
        lat = farm['location']['lat']
        lon = farm['location']['lon']
        
        print(f"\n🏠 Collecting data for {farm_name} ({farm_id})")
        print("-" * 40)
        
        alerts = []
        
        # 1. Weather Data
        weather_data = self.weather_collector.fetch_weather_data(farm_id, lat, lon)
        if weather_data:
            self.weather_collector.save_to_database(weather_data)
        else:
            alerts.append("❌ Weather data collection failed")
        
        # 2. Sensor Data
        if weather_data:
            simulator = self.sensor_simulators[farm_id]
            sensor_data = simulator.get_complete_reading(weather_data)
            if simulator.save_to_database(sensor_data):
                print("✅ Sensor data generated and saved")
            simulator.advance_time(days=COLLECTION_INTERVAL/1440)
        
        # 3. Fire Detection (every 6 hours)
        current_hour = datetime.now().hour
        if current_hour % 6 == 0:
            print("🔥 Running fire detection...")
            fire_incidents = self.fire_detector.fetch_fire_data(farm_id, lat, lon)
            if fire_incidents is not None:
                fire_analysis = self.fire_detector.analyze_fire_risk(farm_id, lat, lon, fire_incidents)
                self.fire_detector.save_fire_analysis(fire_analysis)
                
                if fire_analysis['risk_level'] in ['critical', 'high']:
                    alerts.append(f"🚨 FIRE ALERT: {fire_analysis['risk_level'].upper()} risk!")
                
                print(f"✅ Fire analysis: {fire_analysis['risk_level']} risk")
        
        # 4. Sustainability Scoring (daily)
        if current_hour % 24 == 0:
            print("🌱 Calculating sustainability score...")
            sustainability_analysis = self.sustainability_scorer.calculate_overall_score(farm_id)
            if sustainability_analysis:
                self.sustainability_scorer.save_score(sustainability_analysis)
                
                score = sustainability_analysis['overall_score']
                grade = sustainability_analysis['grade']
                
                if score < 500:
                    alerts.append(f"🌱 LOW SUSTAINABILITY: {score}/1000 ({grade})")
                
                print(f"✅ Sustainability score: {score}/1000 ({grade})")
        
        # Display alerts
        if alerts:
            print("\n🚨 ALERTS:")
            for alert in alerts:
                print(f"   {alert}")
        
        return len(alerts) == 0
    
    def run_collection_cycle(self):
        """Run one complete data collection cycle"""
        print(f"\n🔄 Starting collection cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        successful_collections = 0
        
        for farm in FARMS:
            try:
                success = self.collect_farm_data(farm)
                if success:
                    successful_collections += 1
            except Exception as e:
                print(f"❌ Error with {farm['name']}: {e}")
        
        print(f"\n✅ Collection cycle completed!")
        print(f"   📈 Success: {successful_collections}/{len(FARMS)} farms")
        print(f"   ⏰ Next collection in {COLLECTION_INTERVAL} minutes")
        
        return successful_collections > 0
    
    def run_continuous(self):
        """Run continuous data collection"""
        print(f"\n🔁 Starting continuous collection")
        print(f"   ⏱️  Interval: {COLLECTION_INTERVAL} minutes")
        print(f"   ⭐ Press Ctrl+C to stop")
        print("=" * 60)
        
        cycle_count = 0
        
        try:
            while True:
                cycle_count += 1
                print(f"\n🔄 CYCLE #{cycle_count}")
                
                success = self.run_collection_cycle()
                
                if success:
                    print(f"💤 Sleeping for {COLLECTION_INTERVAL} minutes...")
                    time.sleep(COLLECTION_INTERVAL * 60)
                else:
                    print("⚠️  Some failures. Retrying in 10 minutes...")
                    time.sleep(10 * 60)
                    
        except KeyboardInterrupt:
            print(f"\n\n🛑 Collection stopped by user")
            print(f"📊 Total cycles: {cycle_count}")
            print("👋 Thank you for using Smart AgriChain!")

def main():
    print("🌾 SMART AGRICHAIN DATA COLLECTION")
    print("🤖 AI-powered sustainable agriculture platform")
    print("=" * 60)
    
    if not os.path.exists('database/farm_data.db'):
        print("❌ Database not found!")
        print("🔧 Please run: python scripts/setup_database.py")
        return
    
    manager = DataCollectionManager()
    
    print("\nSelect mode:")
    print("1. Run single collection cycle (test)")
    print("2. Run continuous collection")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == '1':
        manager.run_collection_cycle()
    elif choice == '2':
        manager.run_continuous()
    else:
        print("❌ Invalid choice")

if __name__ == "__main__":
    main()
