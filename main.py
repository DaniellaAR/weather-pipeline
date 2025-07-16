
import time
import pytz  

from handlers               import get_last_timestamp, create_stg_weather_table, create_prod_weather_table
from db_connections         import get_db_connection
from datetime               import datetime, timedelta, timezone
from stg_get_data           import get_one_station, get_all_observations
from stg_load_data          import load_weather_data_stg, stg_extract_load     
from prod_transform_data    import transformed_data   
from prod_load_data         import load_data_enh
from results                import average_temperature, maximum_wind_speed_change, results_queries



if __name__ == "__main__": 

    stg_extract_load() # extract all data from API and then load it into a stagging table

    print(f"\n6. Transformed table, adding new columns")
    transform_data=transformed_data()

    print(f"\n7. Load into prod table with the new columns")
    load_data_enh(transform_data)

    #Results
    results_queries() 


    