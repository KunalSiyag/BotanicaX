import azure.functions as func
import logging
import json
import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared_code'))
from database_helper import CosmosDBHelper

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('BotanicaX Farm Dashboard API triggered')
    
    try:
        farm_id = req.params.get('farm_id')
        if not farm_id:
            return func.HttpResponse(
                json.dumps({"error": "farm_id parameter is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Initialize database
        db = CosmosDBHelper()
        
        # Get dashboard data
        dashboard_data = build_dashboard_data(db, farm_id)
        
        return func.HttpResponse(
            json.dumps(dashboard_data, default=str),
            mimetype="application/json",
            headers={'Access-Control-Allow-Origin': '*'}
        )
        
    except Exception as e:
        logging.error(f"Error in farm dashboard: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

def build_dashboard_data(db: CosmosDBHelper, farm_id: str):
    """Build complete dashboard data for a farm"""
    
    # Get latest sustainability score
    sustainability_score = get_latest_sustainability_score(db, farm_id)
    
    # Get latest weather data
    weather_data = get_latest_weather_data(db, farm_id)
    
    # Get latest sensor data
    sensor_data = get_latest_sensor_data(db, farm_id)
    
    # Get active alerts
    alerts = get_active_alerts(db, farm_id)
    
    dashboard_data = {
        'farm_id': farm_id,
        'timestamp': datetime.utcnow().isoformat(),
        
        # Sustainability metrics
        'sustainability_score': sustainability_score.get('overall_score', 700),
        'sustainability_grade': sustainability_score.get('grade', 'B'),
        
        # Environmental data
        'weather': weather_data,
        'sensor_readings': sensor_data,
        
        # Status & alerts
        'active_alerts': alerts,
        
        # Metadata
        'data_source': 'cosmos_db',
        'last_updated': datetime.utcnow().isoformat()
    }
    
    return dashboard_data

def get_latest_sustainability_score(db: CosmosDBHelper, farm_id: str):
    """Get latest sustainability score"""
    latest_score = db.get_latest_item('sustainability_scores', farm_id)
    return latest_score if latest_score else {}

def get_latest_weather_data(db: CosmosDBHelper, farm_id: str):
    """Get latest weather data"""
    latest_weather = db.get_latest_item('weather_data', farm_id)
    return latest_weather if latest_weather else {}

def get_latest_sensor_data(db: CosmosDBHelper, farm_id: str):
    """Get latest sensor readings"""
    latest_sensor = db.get_latest_item('sensor_data', farm_id)
    return latest_sensor if latest_sensor else {}

def get_active_alerts(db: CosmosDBHelper, farm_id: str):
    """Get active alerts"""
    # Get latest fire alert
    latest_fire_alert = db.get_latest_item('fire_alerts', farm_id)
    
    alerts = []
    if latest_fire_alert and latest_fire_alert.get('risk_level') in ['high', 'critical']:
        alerts.append({
            'type': 'fire_risk',
            'level': latest_fire_alert.get('risk_level'),
            'message': latest_fire_alert.get('recommendation', 'Monitor fire risk'),
            'timestamp': latest_fire_alert.get('timestamp')
        })
    
    return alerts
