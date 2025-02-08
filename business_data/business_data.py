import pandas as pd
import psycopg2
from sqlalchemy import create_engine
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

# Fetch data from the silver schema's customer_order_data table
query = "SELECT * FROM silver.customer_order_data"  # Adjust the table name if needed
customer_order_data = pd.read_sql(query, db_connection)

# Step 3: Filter the data where Product = 'Laptop'
filtered_data = customer_order_data[customer_order_data['Product'] == 'Laptop']

# Step 4: Connect to PostgreSQL to store data in the gold schema's business table
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

# Step 5: Store the filtered data into the gold schema's business table
filtered_data.to_sql('business_data', engine, schema='gold', index=False, if_exists='replace')

# Step 6: Close the database connection
db_connection.close()

print("Filtered data has been successfully stored in the gold.business table!")
