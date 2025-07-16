import requests
from handlers import safe_round
from datetime import datetime, timedelta, timezone
import time
import pytz  # New import for timezone handling


def get_one_station(st_id):
    """Fetch list of weather stations"""
  
    #print(f"\nFor Station id: {st_id}")

    try:
        response = requests.get(
            f"https://api.weather.gov/stations/{st_id}",
            headers={"User-Agent": "assignment"},
           # params={"limit": 1},
            timeout=10
        )
        response.raise_for_status()
        

        station_data = response.json() 

        return {
            'id': station_data['properties']['stationIdentifier'],
            'name': station_data['properties']['name'],
            'timezone': station_data['properties']['timeZone'],
            'latitude': station_data['geometry']['coordinates'][1],
            'longitude': station_data['geometry']['coordinates'][0]
        } #for s in response.json()['features']]
        


    except Exception as e:
        print(f"Error fetching stations: {e}")
        return []




def get_all_observations(station_id, start_date, end_date):
    
    url = f"https://api.weather.gov/stations/{station_id}/observations"
    try:
        response = requests.get(url, headers={"User-Agent": "assignment"})
        response.raise_for_status()
        all_observations = response.json()["features"]
    
    except requests.exceptions.RequestException as e:
        print(f"error gettind the data: {e}")
    

    filtered = []
    for obs in all_observations:

        try:
            obs_time = datetime.fromisoformat(obs["properties"]["timestamp"].replace("Z", "+00:00"))

            if start_date <= obs_time <= end_date:
                filtered.append({
                    "id"            : obs["id"],
                    "timestamp"     : obs["properties"]["timestamp"],
                    "temperature"   : safe_round(obs["properties"]["temperature"], 'value'),
                    "wind_speed"    : safe_round(obs["properties"]["windSpeed"], 'value'),
                    "humidity"      : safe_round(obs["properties"]["relativeHumidity"], 'value')
                })
                
        except (KeyError, TypeError) as e:
            print(f"Error to fetching: {e}")
        
    return filtered