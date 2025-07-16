from db_connections     import get_db_connection
from datetime import datetime, timedelta, timezone
import time
import pytz  # New import for timezone handling
import requests

"""
    create_stg_weather_table()
    create_prod_weather_table()
    safe_round(value_dict, key)
    get_last_timestamp()


"""


def create_stg_weather_table():
    """
    Creates the prod_weather_observations table if it doesn't exist
   
    """
    conn = get_db_connection()

    try:
        # Connect to PostgreSQL
        
        cur = conn.cursor()
        
        # SQL to create weather observations table
        create_stg_weather_table = """
        CREATE TABLE IF NOT EXISTS public.stg_weather_observations (
            id serial PRIMARY KEY,                           -- Auto-incrementing primary key
            station_id varchar(50) NOT NULL,                 -- Weather station identifier
            station_name varchar(100),                       -- Human-readable station name
            station_timezone varchar(50),                    -- Timezone of the station
            station_latitude numeric(9,6),                   -- Latitude coordinate
            station_longitude numeric(9,6),                  -- Longitude coordinate
            obs_timestamp timestamptz,                       -- When observation was taken (with timezone)
            obs_temperature numeric(5,2),                    -- Temperature reading
            obs_wind_speed numeric(5,2),                     -- Wind speed measurement
            obs_humidity numeric(5,2),                       -- Humidity percentage
            load_timestamp timestamptz                       -- When data was loaded into this table
        ) TABLESPACE pg_default;
        """
        
        # Execute the table creation
        cur.execute(create_stg_weather_table)
        print("   Table created successfully stg_weather_table or already exists")

        conn.commit()
        
        
    except Exception as e:
        print(f"Error creating table: {e}")
        if conn:
            conn.rollback()
    finally:
        # Ensure resources are cleaned up
        if conn:
            cur.close()
            conn.close()


def create_prod_weather_table():

    """
    Creates the prod_weather_observations table if it doesn't exist
   
    """

    conn = get_db_connection()

    try:
        # Connect to PostgreSQL
        
        cur= conn.cursor()
        
        # SQL to create weather observations table
        create_prod_weather_table = """
        DELETE FROM prod_weather_observations;  -- Delete data if exist

        CREATE TABLE IF NOT EXISTS public.prod_weather_observations (
            id numeric(1000) PRIMARY KEY,                       
            station_id varchar(50) NOT NULL,                 -- Weather station identifier
            station_name varchar(100),                       -- Human-readable station name
            station_timezone varchar(50),                    -- Timezone of the station
            station_latitude numeric(9,6),                   -- Latitude coordinate
            station_longitude numeric(9,6),                  -- Longitude coordinate
            obs_timestamp timestamptz,                       -- When observation was taken (with timezone)
            obs_temperature numeric(5,2),                    -- Temperature reading
            obs_wind_speed numeric(5,2),                     -- Wind speed measurement
            obs_humidity numeric(5,2),                       -- Humidity percentage
            wind_current numeric(5, 2) ,                     -- Wind speed measurement current
            wind_prev numeric(5, 2) ,                        -- Wind speed measurement previuos
            wind_diff_prct numeric(5, 2) ,                   -- Diff
            load_timestamp timestamptz                       -- When data was loaded into this table
        ) TABLESPACE pg_default;
        """
        
        # Execute the table creation
        cur.execute(create_prod_weather_table)
        print("   Successfully created table: prod_weather_table ")

        conn.commit()
        
        
    except Exception as e:
        print(f"   Error creating table: {e}")
        if conn:
            conn.rollback()
    finally:
        # Ensure resources are cleaned up
        if conn:
            cur.close()
            conn.close()


def safe_round(value_dict, key):
    """Safely rounds API values to 2 decimals or returns None"""

    # 1. Check if the parent dictionary exists and has data
    if value_dict and isinstance(value_dict, dict):
        
        # 2. Safely extract the value
        value = value_dict.get(key)
        
        # 3. Only round if value exists and is numeric
        if value is not None:
            try:
                return round(float(value), 2)  # ← Ensures 2 decimal places
            except (TypeError, ValueError):
                pass  # Handles non-numeric strings
    return None  # Default for missing/invalid data


def get_last_timestamp():
     # Obtener el último timestamp en la DB
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(obs_timestamp) FROM stg_weather_observations")
        result = cursor.fetchone()

        return result[0] if result and result[0] else None


    except Exception as e:
        print(f"Error: {e}")
        return None

