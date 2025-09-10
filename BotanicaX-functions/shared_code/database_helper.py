import os
import logging
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from datetime import datetime
import json
from typing import List, Dict, Optional

class CosmosDBHelper:
    def __init__(self):
        """Initialize Cosmos DB connection"""
        self.connection_string = os.environ.get('COSMOS_DB_CONNECTION')
        self.database_name = os.environ.get('COSMOS_DB_NAME', 'BotanicaXDB')
        
        if not self.connection_string:
            raise ValueError("COSMOS_DB_CONNECTION environment variable not set")
        
        try:
            self.client = CosmosClient.from_connection_string(self.connection_string)
            self.database = self.client.get_database_client(self.database_name)
            logging.info(f"Connected to Cosmos DB: {self.database_name}")
            
            # Initialize containers
            self.init_containers()
            
        except Exception as e:
            logging.error(f"Failed to connect to Cosmos DB: {e}")
            raise
    
    def init_containers(self):
        """Initialize all required containers"""
        containers = [
            ('weather_data', '/farm_id'),
            ('sensor_data', '/farm_id'),
            ('fire_alerts', '/farm_id'),
            ('sustainability_scores', '/farm_id'),
            ('farm_profiles', '/id')
        ]
        
        for container_name, partition_key in containers:
            try:
                self.database.create_container_if_not_exists(
                    id=container_name,
                    partition_key=PartitionKey(path=partition_key),
                    offer_throughput=400
                )
                logging.info(f"Container {container_name} ready")
            except exceptions.CosmosHttpResponseError as e:
                if e.status_code != 409:  # 409 = already exists
                    logging.error(f"Error creating container {container_name}: {e}")
    
    def insert_item(self, container_name: str, item: dict):
        """Insert item into container"""
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
            logging.info(f"Inserted item into {container_name}")
            return result
            
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"Error inserting into {container_name}: {e}")
            raise
    
    def query_items(self, container_name: str, query: str, parameters=None):
        """Query items from container"""
        try:
            container = self.database.get_container_client(container_name)
            
            items = list(container.query_items(
                query=query,
                parameters=parameters or [],
                enable_cross_partition_query=True
            ))
            
            return items
            
        except exceptions.CosmosHttpResponseError as e:
            logging.error(f"Error querying {container_name}: {e}")
            return []
    
    def get_latest_item(self, container_name: str, farm_id: str):
        """Get latest item for a farm"""
        query = "SELECT TOP 1 * FROM c WHERE c.farm_id = @farm_id ORDER BY c.timestamp DESC"
        parameters = [{"name": "@farm_id", "value": farm_id}]
        
        items = self.query_items(container_name, query, parameters)
        return items[0] if items else None
