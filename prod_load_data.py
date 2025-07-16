
import psycopg2
import pytz  # New import for timezone handling
import os
from datetime           import datetime
from db_connections     import get_db_connection
from handlers           import create_prod_weather_table



def load_data_enh(transform_data):
    """
    Loads weather observation data into prod_weather_observations table
    """

    # Create table 
    create_prod_weather_table()

    if not transform_data:
        print("\nNo data provided to load")
        return (0, 0)

    conn = None
    chile_time = datetime.now(pytz.timezone('Chile/Continental'))

    try:
        conn = get_db_connection()
        
        for data in transform_data:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        

                        INSERT INTO prod_weather_observations
                        (id, station_id, station_name, station_timezone, station_latitude, station_longitude,
                         obs_timestamp, obs_temperature, obs_wind_speed, obs_humidity,
                         wind_current, wind_prev, wind_diff_prct, load_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            data["id"],
                            data["station_id"],
                            data["station_name"],
                            data["station_timezone"],
                            data["station_latitude"],
                            data["station_longitude"],
                            data["obs_timestamp"],
                            data["obs_temperature"],
                            data["obs_wind_speed"],
                            data["obs_humidity"],
                            data["wind_current"],
                            data["wind_prev"],
                            data["wind_diff_prct"],
                            chile_time
                        )
                        
                    )
                 
                    
            except KeyError as e:
                print(f"Missing field {str(e)} in record {data.get('id', 'unknown')}")
                continue
            except Exception as e:
                print(f"Error inserting record {data.get('id', 'unknown')}: {str(e)}")
                continue
 
        conn.commit()
        print(f"\n   Successfully loaded records into prod_weather_table")
        return True

    except Exception as e:
        print(f"Database connection error: {str(e)}")
        if conn:
            conn.rollback()
        return (success, errors)
    finally:
        if conn:
            conn.close()