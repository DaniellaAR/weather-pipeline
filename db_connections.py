import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")


def get_db_connection():
    """Establish and return a database connection"""
    try:
        conn = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

