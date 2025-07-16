# Weather Data Pipeline

A Python ETL pipeline that processes weather observations, analyzes wind speed variations, and loads data to Supabase.

## Features
- Extracts weather data from API sources
- Filters last 7 days of observations
- Performs incremental loads
- Calculates wind speed variations
- Stores data in Supabase (PostgreSQL)

## Dependencies
```python
# Core Imports
import psycopg2
from supabase import create_client
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv
