from db_connections     import get_db_connection
import requests


def transformed_data():
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        cursor.execute(
        """
            WITH data as (
                SELECT 
                        *,
                        obs_wind_speed as wind_current,
                        LAG(obs_wind_speed) OVER (ORDER BY obs_timestamp) AS wind_prev
                    FROM stg_weather_observations
                    order by obs_timestamp desc
                    )

            select 
                    id,
                    station_id,
                    station_name,
                    station_timezone,
                    station_latitude,
                    station_longitude,
                    obs_timestamp,
                    obs_temperature,
                    obs_wind_speed,
                    obs_humidity,
                    wind_current,
                    wind_prev,
                CASE 
                    WHEN wind_prev IS NULL OR wind_prev = 0 THEN NULL
                    ELSE round(abs( (wind_current - wind_prev) / wind_prev),2)
                END AS wind_diff_prct
                FROM  data 
                ORDER BY  date(obs_timestamp) DESC
   
        """)

            
        column_names = [desc[0] for desc in cursor.description]
        #print(f"Columns names ", column_names)
        
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(column_names, row)))
                
        return results

    except Exception as e:
        print(f"Error: {e}")
        return None
