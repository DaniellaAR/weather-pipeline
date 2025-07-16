# Weather Data Pipeline

ETL pipeline for processing weather observations with wind speed analysis, built with Python and Supabase.

## Table of Contents
- [Features](#-features)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Pipeline Structure](#-pipeline-structure)
- [Usage](#-usage)
- [yaml](#-yaml)

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


##Overview
This automated pipeline processes weather observation data through three distinct environments, implementing incremental updates and analytical calculations. The workflow executes every 6 hours via GitHub Actions.

# Weather Data Processing Pipeline

## Overview
This automated pipeline processes weather observation data through a three-stage workflow, implementing incremental updates and analytical calculations. The system executes every 6 hours via GitHub Actions.

## Pipeline Architecture

### Data Environments

| Environment        | Table Name                   | Purpose                                                                 |
|--------------------|------------------------------|-------------------------------------------------------------------------|
| **Raw**           | (External Source)            | Contains source data in original format from weather APIs               |
| **Staging**       | `stg_weather_observations`   | Stores cleansed, deduplicated observation records                       |
| **Production**    | `prod_weather_observations`  | Contains analysis-ready data with calculated metrics                    |

## Workflow Logic

### Incremental Data Loading
The pipeline implements smart data collection:

```python
# Gets last observation or defaults to 7 days ago
last_date = get_last_observation_date()
start_date = last_date if last_date else datetime.now() - timedelta(days=7)



#YAML file 









