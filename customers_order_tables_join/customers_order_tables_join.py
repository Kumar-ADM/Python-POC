import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
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

# Step 3: Connect to PostgreSQL
db_connection = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password
)

# Step 4: Ensure schema exists before inserting data
schema_name = 'silver'

with db_connection.cursor() as cursor:
    cursor.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}';")
    schema_exists = cursor.fetchone()

    if not schema_exists:
        print(f"Schema '{schema_name}' does not exist. Creating schema...")
        cursor.execute(f"CREATE SCHEMA {schema_name};")
        db_connection.commit()
    else:
        print(f"Schema '{schema_name}' already exists.")

# Step 5: Fetch customer data from PostgreSQL
customer_query = "SELECT * FROM bronze.customers"  # Adjust the table name if needed
customer_data = pd.read_sql(customer_query, db_connection)

# Step 6: Join the Order data and Customer data on customerid
merged_data = pd.merge(order_data, customer_data, on='customerid', how='inner')

# Step 7: Store the joined data into the specified schema
table_name = "customer_order_data"  # Only table name (schema will be used in query)

# Create the SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

# Write data to PostgreSQL (this will overwrite the table, use 'append' if needed)
merged_data.to_sql(table_name, engine, schema=schema_name, index=False, if_exists='replace')

# Close the database connection
db_connection.close()

print(f"Data has been successfully joined and stored in '{schema_name}.{table_name}' in PostgreSQL!")