import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import configparser

# Step 1: Read configuration from config.ini
config = configparser.ConfigParser()
config.read(r"C:\Users\siva\OneDrive\Documents\Databricks\DAPS_POC\Python-POC\load_csv_to_postgres\config.ini")

# Retrieve PostgreSQL credentials
host = config['pgsql']['host']
port = config['pgsql']['port']
dbname = config['pgsql']['dbname']
user = config['pgsql']['user']
password = config['pgsql']['password']
schema = config['pgsql']['schema']

# Step 2: Connect to PostgreSQL to fetch data from the silver schema
db_connection = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password
)

# Step 3: Ensure `gold` schema exists before inserting data
gold_schema = "gold"

with db_connection.cursor() as cursor:
    cursor.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{gold_schema}';")
    schema_exists = cursor.fetchone()

    if not schema_exists:
        print(f"Schema '{gold_schema}' does not exist. Creating schema...")
        cursor.execute(f"CREATE SCHEMA {gold_schema};")
        db_connection.commit()
    else:
        print(f"Schema '{gold_schema}' already exists.")

# Step 4: Fetch data from `silver.customer_order_data` table
query = "SELECT * FROM silver.customer_order_data"
customer_order_data = pd.read_sql(query, db_connection)

# Step 5: Filter the data where Product = 'Laptop'
filtered_data = customer_order_data[customer_order_data['product'] == 'Laptop']

# Step 6: Connect to PostgreSQL using SQLAlchemy for writing data
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

# Step 7: Store the filtered data into `gold.business_data` table
filtered_data.to_sql('business_data', engine, schema=gold_schema, index=False, if_exists='replace')

# Step 8: Close the database connection
db_connection.close()

print(f"Filtered data has been successfully stored in '{gold_schema}.business_data' table!")
