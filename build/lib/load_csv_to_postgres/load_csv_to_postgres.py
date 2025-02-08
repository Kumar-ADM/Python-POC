import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini")

# Read database connection details
db_host = config["database"]["host"]
db_user = config["database"]["user"]
db_password = config["database"]["password"]
db_name = config["database"]["dbname"]
db_schema = config["database"]["schema"]
csv_file_path = config["file"]["csv_path"]

# Create PostgreSQL connection
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}")
conn = psycopg2.connect(
    host=db_host, database=db_name, user=db_user, password=db_password
)
cursor = conn.cursor()

# Ensure schema exists
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")

# Create table if not exists
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {db_schema}.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
cursor.execute(create_table_query)
conn.commit()

# Load CSV into DataFrame
df = pd.read_csv(csv_file_path)

# Load data into PostgreSQL
df.to_sql("customers", engine, schema=db_schema, if_exists="append", index=False)

print("CSV data successfully loaded into PostgreSQL table bronze.customers.")

# Close connections
cursor.close()
conn.close()