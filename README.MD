# Weather Data Pipeline

ETL pipeline for processing weather observations with wind speed analysis, built with Python and Supabase.

## Table of Contents
- [Features](#-features)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Pipeline Structure](#-pipeline-structure)
- [Usage](#-usage)
- [Data Model](#-data-model)

## Features
- API data extraction
- 7-day rolling window processing
- Wind speed differential analysis
- Supabase PostgreSQL integration
- Incremental loading

##  Installation
```bash
# Clone repository
git clone https://github.com/DaniellaAR/weather-pipeline.git
cd weather-pipeline

#set up enviroment 
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

# Install dependencies
Create .env file, replacing with the data that will be share in the email
```bash
user=
password=
host=
port=
dbname=
```

# Pipeline structure

it get all the observation for one station 
if there is no data in stg_weather_observations then it will filter and load the last 7 days, otherwise will only update the most recent on 

in the second part with sql code will create 3 columns, this is to do get the differences vs the previus one using LAG() functions 
and then this is sabe in a new table, prod_weather_observations

result code will show the queries done under the prod_weather_observations table. 

This approach was taking to have in consideration that it should be 3 enviroments, raw, stagging and production, 
raw: data as it is 
stagging: modify data, in this case was to create the columns, so the analysis will be easy to be done. 
prod: this enviroment it is for the analysis, that it will have the most clean and updated data possible. 










