import os
import sys
import json
from pathlib import Path

# Add function path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'BotanicaX-functions', 'shared_code'))

def load_local_settings():
    """Load environment variables from local.settings.json"""
    settings_path = Path(__file__).parent.parent / 'BotanicaX-functions' / 'local.settings.json'
    
    if settings_path.exists():
        with open(settings_path, 'r') as f:
            settings = json.load(f)
            
        for key, value in settings.get('Values', {}).items():
            os.environ[key] = value
            
        print(f"✅ Loaded settings from {settings_path}")
    else:
        print(f"❌ Settings file not found at {settings_path}")

def setup_cosmos_containers():
    """Create all required Cosmos DB containers"""
    
    load_local_settings()
    
    try:
        from azure.cosmos import CosmosClient, PartitionKey, exceptions
        
        # Get connection details
        connection_string = os.environ.get('COSMOS_DB_CONNECTION')
        database_name = os.environ.get('COSMOS_DB_NAME', 'BotanicaXDB')
        
        if not connection_string:
            print("❌ COSMOS_DB_CONNECTION not found in environment variables")
            return False
        
        # Connect to Cosmos DB
        client = CosmosClient.from_connection_string(connection_string)
        
        # Create database if it doesn't exist
        try:
            database = client.create_database_if_not_exists(id=database_name)
            print(f"✅ Database '{database_name}' ready")
        except Exception as e:
            print(f"❌ Error creating database: {e}")
            return False
        
        # Define containers with their configurations
        containers_config = [
            {
                'name': 'sensor_data',
                'partition_key': '/farm_id',
                'throughput': 400,
                'description': 'Stores all sensor readings (soil, air quality, weather stations)'
            },
            {
                'name': 'weather_data',
                'partition_key': '/farm_id',
                'throughput': 400,
                'description': 'Stores weather data from OpenWeatherMap API'
            },
            {
                'name': 'fire_alerts',
                'partition_key': '/farm_id',
                'throughput': 400,
                'description': 'Stores fire risk alerts from NASA FIRMS'
            },
            {
                'name': 'sustainability_scores',
                'partition_key': '/farm_id',
                'throughput': 400,
                'description': 'Stores daily sustainability score calculations'
            },
            {
                'name': 'farm_profiles',
                'partition_key': '/id',
                'throughput': 400,
                'description': 'Stores farm profile information and metadata'
            }
        ]
        
        # Create each container
        for container_config in containers_config:
            try:
                container = database.create_container_if_not_exists(
                    id=container_config['name'],
                    partition_key=PartitionKey(path=container_config['partition_key']),
                    offer_throughput=container_config['throughput']
                )
                
                print(f"✅ Container '{container_config['name']}' ready")
                print(f"   - Partition key: {container_config['partition_key']}")
                print(f"   - Throughput: {container_config['throughput']} RU/s")
                print(f"   - Purpose: {container_config['description']}")
                
            except exceptions.CosmosHttpResponseError as e:
                if e.status_code == 409:  # Container already exists
                    print(f"✅ Container '{container_config['name']}' already exists")
                else:
                    print(f"❌ Error creating container {container_config['name']}: {e}")
                    return False
        
        print("\n🎉 All Cosmos DB containers are now ready!")
        print("You can now run your fake data generation script.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error setting up Cosmos DB containers: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Setting up BotanicaX Cosmos DB containers...")
    success = setup_cosmos_containers()
    
    if success:
        print("\n✅ Setup completed successfully!")
        print("Next steps:")
        print("1. Run 'python scripts/generate_fake_data.py' to populate test data")
        print("2. Test your APIs with 'curl' commands")
        print("3. Deploy your Azure Functions")
    else:
        print("\n❌ Setup failed. Please check your Cosmos DB connection and try again.")
