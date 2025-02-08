import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read(r"C:\Users\siva\OneDrive\Documents\Databricks\DAPS_POC\Python-POC\load_csv_to_postgres\config.ini")


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

# Check if schema exists before creating it
cursor.execute("""
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name = %s
""", [db_schema])

# Fetch the result of the query
schema_exists = cursor.fetchone()

# If schema doesn't exist, create it
if not schema_exists:
    cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(db_schema)))
    print(f"Schema '{db_schema}' created.")

# Create table if not exists
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {db_schema}.customers (
    CustomerID SERIAL PRIMARY KEY,
    FirstName VARCHAR(255),
    LastName VARCHAR(255),
    Email VARCHAR(255) UNIQUE,
    Phone VARCHAR(50),
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