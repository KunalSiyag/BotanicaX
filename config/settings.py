# config/settings.py

API_KEYS = {
    'openweather': '9ea9c5a5b723eafc3de84001699d2f6b',  # Get from openweathermap.org
    'nasa_firms': '004813bb11be9cfc670b0e6d8629f1ba',         # Get from firms.modaps.eosdis.nasa.gov
}

FARMS = [
    {
        'id': 'FARM001',
        'name': 'Demo Farm Visakhapatnam',
        'location': {'lat': 17.6868, 'lon': 83.2185},
        'profile': {
            'crop': 'rice',
            'soil_type': 'black_soil',
            'farming_type': 'organic',
            'area_hectares': 2.5,
            'owner': 'Demo Farmer'
        }
    },
    {
        'id': 'FARM002',
        'name': 'Demo Farm Delhi',
        'location': {'lat': 28.7041, 'lon': 77.1025},
        'profile': {
            'crop': 'wheat',
            'soil_type': 'loamy',
            'farming_type': 'conventional',
            'area_hectares': 1.8,
            'owner': 'Demo Farmer 2'
        }
    }
]

DATABASE_PATH = 'database/farm_data.db'
COLLECTION_INTERVAL = 60  # minutes
