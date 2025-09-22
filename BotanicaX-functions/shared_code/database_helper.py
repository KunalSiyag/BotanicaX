import os
import logging
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from datetime import datetime
import json
from typing import List, Dict, Optional

class CosmosDBHelper:
    def __init__(self):
        """Initialize Cosmos DB connection with better error handling"""
        self.connection_string = os.environ.get('COSMOS_DB_CONNECTION')
        self.database_name = os.environ.get('COSMOS_DB_NAME', 'Botanicax')
        
        if not self.connection_string:
            raise ValueError("❌ COSMOS_DB_CONNECTION environment variable not set")
        
        try:
            self.client = CosmosClient.from_connection_string(self.connection_string)
            
            # Create database if it doesn't exist
            self.database = self.client.create_database_if_not_exists(id=self.database_name)
            logging.info(f"✅ Connected to Cosmos DB: {self.database_name}")
            
            # Verify containers exist
            self.verify_containers()
            
        except Exception as e:
            logging.error(f"❌ Failed to connect to Cosmos DB: {e}")
            raise
    
    def verify_containers(self):
        """Verify that all required containers exist"""
        required_containers = [
            ('sensor_data', '/farm_id'),
            ('weather_data', '/farm_id'),
            ('fire_alerts', '/farm_id'),
            ('sustainability_scores', '/farm_id'),
            ('farm_profiles', '/id')
        ]
        
        missing_containers = []
        
        for container_name, partition_key in required_containers:
            try:
                container = self.database.get_container_client(container_name)
                container.read()  # Test if container exists
                logging.info(f"✅ Container '{container_name}' exists")
            except exceptions.CosmosResourceNotFoundError:
                missing_containers.append((container_name, partition_key))
                logging.warning(f"⚠️ Container '{container_name}' not found")
        
        if missing_containers:
            print("❌ Missing Cosmos DB containers detected!")
            print("Please create the following containers:")
            for container_name, partition_key in missing_containers:
                print(f"  - {container_name} (partition key: {partition_key})")
            print("\nRun 'python scripts/setup_cosmos_containers.py' to create them automatically.")
            raise ValueError(f"Missing containers: {[c[0] for c in missing_containers]}")
    
    def insert_item(self, container_name: str, item: dict) -> dict:
        """Insert item into container with better error handling"""
        try:
            container = self.database.get_container_client(container_name)
            
            # Add timestamp if not present
            if 'timestamp' not in item:
                item['timestamp'] = datetime.utcnow().isoformat()
            
            # Generate ID if not present
            if 'id' not in item:
                timestamp_clean = item['timestamp'].replace(':', '').replace('-', '').replace('.', '')
                item['id'] = f"{item.get('farm_id', 'unknown')}_{timestamp_clean}"
            
            result = container.create_item(body=item)
            logging.debug(f"✅ Inserted item into {container_name}")
            return result
            
        except exceptions.CosmosResourceNotFoundError:
            logging.error(f"❌ Container '{container_name}' not found. Please create it first.")
            raise
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"❌ Cosmos DB error inserting into {container_name}: {e}")
            raise
        except Exception as e:
            logging.error(f"❌ Unexpected error inserting into {container_name}: {e}")
            raise
    
    def query_items(self, container_name: str, query: str, parameters: List[Dict] = None) -> List[Dict]:
        """Query items from container"""
        try:
            container = self.database.get_container_client(container_name)
            
            items = list(container.query_items(
                query=query,
                parameters=parameters or [],
                enable_cross_partition_query=True
            ))
            
            return items
            
        except exceptions.CosmosResourceNotFoundError:
            logging.error(f"❌ Container '{container_name}' not found")
            return []
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"❌ Error querying {container_name}: {e}")
            return []
    
    def get_latest_item(self, container_name: str, farm_id: str) -> Optional[Dict]:
        """Get latest item for a farm"""
        query = "SELECT TOP 1 * FROM c WHERE c.farm_id = @farm_id ORDER BY c.timestamp DESC"
        parameters = [{"name": "@farm_id", "value": farm_id}]
        
        items = self.query_items(container_name, query, parameters)
        return items[0] if items else None
