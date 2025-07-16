from db_connections     import get_db_connection
import requests


def average_temperature():
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        cursor.execute(
        """

            WITH last_monday AS (
            SELECT CURRENT_DATE - CAST(EXTRACT(DOW FROM CURRENT_DATE) + 6 AS INT) % 7 AS date
            ),

            dates_f as (
            SELECT
            (date - 7) as last_monday,
            (date-1) as last_sunday
            FROM last_monday
            )

            select
            --min(date(obs_timestamp)) as last_monday,
            --max(date(obs_timestamp)) as last_sunday,
            round( avg(obs_temperature),2) as average_temp
            from stg_weather_observations as wo
            left join dates_f as d
            on 1=1
            where ( date(obs_timestamp) >= d.last_monday and date(obs_timestamp) <= d.last_sunday )


        """)

        result = cursor.fetchone()
        return result[0] if result else None

    except Exception as e:
        print(f"Error: {e}")
        return None



def maximum_wind_speed_change():
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        cursor.execute(
        """
            SELECT Max(wind_diff_prct) as maximum_difference
            from prod_weather_observations
        """)

        
        result = cursor.fetchone()
        return result[0] if result else None

    except Exception as e:
        print(f"Error: {e}")
        return None

def results_queries():
    print(f"\n8. Results")
    avg_temp= average_temperature()
    print(f"\n   Average Temperature for the last week is: {avg_temp}" ) #if the last week is not complete, the data is from start date of the data until the last sunday

    max_wind_var= maximum_wind_speed_change()
    print(f"\n   Maximum wind speed change for the last 7 days is: {max_wind_var}" ) #if the last week is not complete, the data is from start date of the data until the last sunday
