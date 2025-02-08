import pytest
import pandas as pd
import configparser
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine

# Load the function to test
import load_csv_to_postgres

@pytest.fixture
def mock_config(mocker):
    """Mock the config.ini file"""
    mock_config = mocker.patch("configparser.ConfigParser")
    config_instance = mock_config.return_value
    config_instance.read.return_value = None
    config_instance.__getitem__.side_effect = lambda section: {
        "host": "test_host",
        "port": "5432",
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
        "schema": "bronze",
    } if section == "database" else {"csv_path": "test.csv"}
    return config_instance

@pytest.fixture
def mock_postgres_connection(mocker):
    """Mock PostgreSQL database connection"""
    mock_conn = mocker.patch("psycopg2.connect")
    mock_cursor = MagicMock()
    mock_conn.return_value.cursor.return_value = mock_cursor
    return mock_cursor

@pytest.fixture
def mock_sqlalchemy_engine(mocker):
    """Mock SQLAlchemy engine"""
    mock_engine = mocker.patch("sqlalchemy.create_engine")
    return mock_engine

@pytest.fixture
def mock_pandas_read_csv(mocker):
    """Mock pandas read_csv"""
    mock_df = pd.DataFrame({
        "name": ["John Doe", "Jane Smith"],
        "email": ["johndoe@example.com", "janesmith@example.com"],
        "phone": ["1234567890", "9876543210"]
    })
    mocker.patch("pandas.read_csv", return_value=mock_df)
    return mock_df

def test_load_csv_to_postgres(mock_config, mock_postgres_connection, mock_sqlalchemy_engine, mock_pandas_read_csv):
    """Test CSV loading function"""
    
    # Run the script (which should now use mocks)
    load_csv_to_postgres

    # Check if the schema and table creation commands were executed
    mock_postgres_connection.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS bronze")
    mock_postgres_connection.execute.assert_any_call("""
        CREATE TABLE IF NOT EXISTS bronze.customers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255) UNIQUE,
            phone VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Check if pandas tried to load the CSV
    pd.read_csv.assert_called_once_with("test.csv")

    # Check if data was inserted using pandas `.to_sql()`
    mock_sqlalchemy_engine.return_value.connect.return_value.execute.assert_called()

    print("Test passed successfully!")