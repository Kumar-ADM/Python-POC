import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import configparser

# Step 1: Read configuration from config.ini
config = configparser.ConfigParser()
config.read(r"C:\Users\siva\OneDrive\Documents\Databricks\DAPS_POC\Python-POC\load_csv_to_postgres\config.ini")

# Retrieve PostgreSQL credentials
host = config['database']['host']
port = config['database']['port']
dbname = config['database']['dbname']
user = config['database']['user']
password = config['database']['password']

# Retrieve the CSV file path
csv_path = config['file']['order_csv_path']

# Step 2: Read Order data from the CSV (path taken from config.ini)
order_data = pd.read_csv(csv_path)  # Read the order data from CSV

# Step 3: Connect to PostgreSQL to fetch customer data
db_connection = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password
)

# Fetch customer data from PostgreSQL
customer_query = "SELECT * FROM bronze.customers"  # Adjust the table name if needed
customer_data = pd.read_sql(customer_query, db_connection)

# Step 4: Join the Order data and Customer data on CustomerID
merged_data = pd.merge(order_data, customer_data, on='customerid', how='inner')

# Step 5: Store the joined data into a different schema in PostgreSQL
# Specify the schema (e.g., 'new_schema') in the table name
schema_name = 'silver'
table_name = f"{schema_name}.customer_order_data"  # Full table name with schema

# Create the SQLAlchemy engine for the connection
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

# Write data to PostgreSQL (this will overwrite the table, use 'append' if you prefer)
merged_data.to_sql(table_name, engine, index=False, if_exists='replace')

# Close the database connection
db_connection.close()

print(f"Data has been successfully joined and stored in the {schema_name} schema in PostgreSQL!")
