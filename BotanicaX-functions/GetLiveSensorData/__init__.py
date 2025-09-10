import azure.functions as func
import logging
import json
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared_code'))
from database_helper import CosmosDBHelper

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('BotanicaX GetLiveSensorData API triggered')
    
    try:
        farm_id = req.params.get('farm_id')
        hours = int(req.params.get('hours', 24))
        
        if not farm_id:
            return func.HttpResponse(
                json.dumps({"error": "farm_id parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Initialize database
        db = CosmosDBHelper()
        
        # Get live sensor data
        live_data = get_live_sensor_data(db, farm_id, hours)
        
        return func.HttpResponse(
            json.dumps(live_data, default=str),
            mimetype="application/json",
            headers={'Access-Control-Allow-Origin': '*'}
        )
        
    except Exception as e:
        logging.error(f"Error in GetLiveSensorData: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

def get_live_sensor_data(db: CosmosDBHelper, farm_id: str, hours: int):
    """Get live sensor data for the specified time window"""
    
    time_window = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    
    # Get sensor readings
    sensor_query = """
    SELECT * FROM c 
    WHERE c.farm_id = @farm_id 
    AND c.timestamp >= @time_window
    ORDER BY c.timestamp DESC
    """
    
    parameters = [
        {"name": "@farm_id", "value": farm_id},
        {"name": "@time_window", "value": time_window}
    ]
    
    sensor_readings = db.query_items('sensor_data', sensor_query, parameters)
    
    # Get weather data
    weather_readings = db.query_items('weather_data', sensor_query, parameters)
    
    live_data = {
        'farm_id': farm_id,
        'query_timestamp': datetime.utcnow().isoformat(),
        'time_window_hours': hours,
        
        # Current readings (latest values)
        'current_readings': {
            'soil': get_latest_by_type(sensor_readings, 'soil'),
            'weather_station': get_latest_by_type(sensor_readings, 'weather_station'),
            'air_quality': get_latest_by_type(sensor_readings, 'air_quality')
        },
        
        # Historical data for trends
        'sensor_history': sensor_readings[:20],  # Limit to 20 recent readings
        'weather_history': weather_readings[:10],  # Limit to 10 recent readings
        
        # Metadata
        'total_readings': len(sensor_readings),
        'data_source': 'cosmos_db'
    }
    
    return live_data

def get_latest_by_type(readings: list, sensor_type: str):
    """Get the most recent reading for a specific sensor type"""
    for reading in readings:
        if reading.get('sensor_type') == sensor_type:
            return reading
    return None
