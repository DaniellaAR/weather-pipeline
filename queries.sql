--drop table stg_weather_observations 


/* Create SQL tables */  
drop table stg_weather_observations 
drop table prod_weather_observations
delete from  if exists prod_weather_observations

create table public.stg_weather_observations (
  id serial not null,
  station_id character varying(50) not null,
  station_name character varying(100) null,
  station_timezone character varying(50) null,
  station_latitude numeric(9, 6) null,
  station_longitude numeric(9, 6) null,
  obs_timestamp timestamp with time zone null,
  obs_temperature numeric(5, 2) null,
  obs_wind_speed numeric(5, 2) null,
  obs_humidity numeric(5, 2) null,
  load_timestamp timestamp with time zone null,
  constraint stg_weather_observations_pkey primary key (id)
) TABLESPACE pg_default;

create table public.prod_weather_observations (
  id numeric not null,
  station_id character varying(50) not null,
  station_name character varying(100) null,
  station_timezone character varying(50) null,
  station_latitude numeric(9, 6) null,
  station_longitude numeric(9, 6) null,
  obs_timestamp timestamp with time zone null,
  obs_temperature numeric(5, 2) null,
  obs_wind_speed numeric(5, 2) null,
  obs_humidity numeric(5, 2) null,
  wind_current numeric(5, 2) null,
  wind_prev numeric(5, 2) null,
  wind_diff_prct numeric(5, 2) null,
  load_timestamp timestamp with time zone null,

  constraint prod_weather_observations_pkey primary key (id)
) TABLESPACE pg_default;


/* Adding new columns */  
            with data as (
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

/* Average temp for the last week */  

-- For any date
-- Using strftime (Sunday=0 to Saturday=6)
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
min(date(observation_time)) as last_monday,
max(date(observation_time)) as last_sunday,
round( avg(observation_temperature),2) as average_temp
from stg_weather_observations as wo
left join dates_f as d
  on 1=1
where ( date(observation_time) >= d.last_monday and
date(observation_time) <= d.last_sunday )

/* Average temp for the last 7 days*/  

select  Max(wind_diff_prct) as maximum_difference
from prod_weather_observations
