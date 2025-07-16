import psycopg2
import pytz  # New import for timezone handling
import os
from db_connections     import get_db_connection
from datetime import datetime, timedelta, timezone
from handlers           import create_stg_weather_table, get_last_timestamp
from stg_get_data       import get_one_station, get_all_observations


#load data filtered for the last 7 days
def load_weather_data_stg(station_data, observation_data):
    conn = get_db_connection()
    create_stg_weather_table()

    try:
       
        if not observation_data or not isinstance(observation_data, list):
            print(f"\n   Data is up to date in Stagging\n")
            return False

        for obs in observation_data:
            print(obs)
            try:

                # Proceed only if we have valid data for this observation
                if not obs.get('timestamp'):
                    print(f"\nSkipping observation - Missing timestamp for station {station_data['id']}")
                    continue

                
                chile_tz = pytz.timezone('Chile/Continental') # to know when has been the last update
                chile_time = datetime.now(chile_tz)
                
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO stg_weather_observations
                        (station_id, station_name, station_timezone, station_latitude, station_longitude,
                        obs_timestamp, obs_temperature, obs_wind_speed, obs_humidity, load_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            
                            station_data['id'],
                            station_data['name'],
                            station_data['timezone'],
                            station_data['latitude'],
                            station_data['longitude'],
                            obs['timestamp'],
                            obs.get('temperature'),
                            obs.get('wind_speed'),
                            obs.get('humidity'),
                            chile_time
                        )
                    )
                
                #print(f"\n Weather observation has been added for station {station_data['id']} at { max(obs['timestamp']) }")

            except Exception as e:
                print(f"\nError saving observation for station {station_data['id']}: {e}")
                conn.rollback()
                continue  # Continue with next observation instead of returning

        

        conn.commit()
        return True

    except Exception as e:
        print(f"\nDatabase error for station {station_data['id']}: {e}")
        conn.rollback()
        return False

    finally:
        conn.close()


def extract_data_pipe(station_id):
    conn = get_db_connection()

    stations = get_one_station(station_id)
    print(f"\n2. Get observations for station id: {stations["id"]}")

    end_date = datetime.now(timezone.utc)  #print(f"End date: {end_date}")

    last_timestamp = get_last_timestamp() #print(f"Last time stamp: {last_timestamp}")

    days=7
   
    if last_timestamp is None:
      
        start_date = end_date - timedelta(days) #end_date   = end_date - timedelta(days= 2)


        print(f"\n3. Historical load")
        print(f"   Start date : {start_date}")
        print(f"   End date   : {end_date}")

        #observations = get_all_observations(station_id , start_date, end_date) 
       
        print(f"\n   Historical data for the last {days} days as been updated")
        print(f"\n")
    else:
     
        start_date = last_timestamp + timedelta(minutes=1)

        print(f"\n3. Incremental Load")
        print(f"   Start date: {start_date}")
        print(f"   End date  : {end_date}")

        
        observations = get_all_observations(station_id , start_date, end_date) 

        
    load_weather_data_stg(stations, observations)
    conn.close()


def stg_extract_load():

    station_id = "001CE" # 001CE

    print(f"\n")
    print(f"\n1. Extract data from Stations and Observations")
    extract_data_pipe(station_id) #extraction and load into a stg table





