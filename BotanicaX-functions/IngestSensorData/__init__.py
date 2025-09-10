import azure.functions as func
import logging
import json
import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared_code'))
from database_helper import CosmosDBHelper

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('BotanicaX Sensor Data Ingestion triggered')
    
    try:
        # Parse request body
        try:
            req_body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                json.dumps({"error": "Invalid JSON in request body"}),
                status_code=400,
                mimetype="application/json"
            )
        
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Validate required fields
        required_fields = ['farm_id', 'sensor_type']
        for field in required_fields:
            if field not in req_body:
                return func.HttpResponse(
                    json.dumps({"error": f"Missing required field: {field}"}),
                    status_code=400,
                    mimetype="application/json"
                )
        
        # Initialize database
        db = CosmosDBHelper()
        
        # Process sensor data
        sensor_processor = SensorDataProcessor(db)
        result = sensor_processor.process_sensor_data(req_body)
        
        if result['success']:
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "message": "Sensor data ingested successfully",
                    "data_id": result.get('data_id')
                }),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({
                    "status": "error", 
                    "message": result.get('error', 'Unknown error')
                }),
                status_code=500,
                mimetype="application/json"
            )
    
    except Exception as e:
        logging.error(f"Error in sensor data ingestion: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

class SensorDataProcessor:
    def __init__(self, db: CosmosDBHelper):
        self.db = db
    
    def process_sensor_data(self, sensor_data: dict):
        """Process and store sensor data based on type"""
        
        sensor_type = sensor_data['sensor_type']
        farm_id = sensor_data['farm_id']
        
        try:
            if sensor_type == 'soil':
                return self._process_soil_data(sensor_data)
            elif sensor_type == 'weather_station':
                return self._process_weather_station_data(sensor_data)
            elif sensor_type == 'air_quality':
                return self._process_air_quality_data(sensor_data)
            else:
                return {
                    'success': False,
                    'error': f'Unknown sensor type: {sensor_type}'
                }
                
        except Exception as e:
            logging.error(f"Error processing {sensor_type} data for farm {farm_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _process_soil_data(self, data: dict):
        """Process soil sensor data"""
        
        # Validate soil sensor fields
        required_fields = ['soil_moisture', 'soil_temperature', 'soil_ph']
        for field in required_fields:
            if field not in data:
                return {
                    'success': False,
                    'error': f'Missing soil sensor field: {field}'
                }
        
        # Enrich soil data
        soil_data = {
            'farm_id': data['farm_id'],
            'sensor_type': 'soil',
            'soil_moisture': float(data['soil_moisture']),
            'soil_temperature': float(data['soil_temperature']),
            'soil_ph': float(data['soil_ph']),
            'nitrogen': float(data.get('nitrogen', 0)),
            'phosphorus': float(data.get('phosphorus', 0)),
            'potassium': float(data.get('potassium', 0)),
            'organic_carbon': float(data.get('organic_carbon', 0)),
            'device_id': data.get('device_id', 'unknown'),
            'battery_level': data.get('battery_level', 100),
            'data_source': 'iot_sensor',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Store in Cosmos DB
        result = self.db.insert_item('sensor_data', soil_data)
        
        return {
            'success': True,
            'data_id': result['id']
        }
    
    def _process_weather_station_data(self, data: dict):
        """Process weather station data"""
        
        weather_data = {
            'farm_id': data['farm_id'],
            'sensor_type': 'weather_station',
            'temperature': float(data.get('temperature', 0)),
            'humidity': float(data.get('humidity', 0)),
            'pressure': float(data.get('pressure', 1013)),
            'wind_speed': float(data.get('wind_speed', 0)),
            'rainfall': float(data.get('rainfall', 0)),
            'device_id': data.get('device_id', 'unknown'),
            'data_source': 'weather_station',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        result = self.db.insert_item('sensor_data', weather_data)
        
        return {
            'success': True,
            'data_id': result['id']
        }
    
    def _process_air_quality_data(self, data: dict):
        """Process air quality sensor data"""
        
        air_quality_data = {
            'farm_id': data['farm_id'],
            'sensor_type': 'air_quality',
            'co2': float(data.get('co2', 0)),
            'ch4': float(data.get('ch4', 0)),
            'n2o': float(data.get('n2o', 0)),
            'pm25': float(data.get('pm25', 0)),
            'device_id': data.get('device_id', 'unknown'),
            'data_source': 'air_quality_sensor',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        result = self.db.insert_item('sensor_data', air_quality_data)
        
        return {
            'success': True,
            'data_id': result['id']
        }
