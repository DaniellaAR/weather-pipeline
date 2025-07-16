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





