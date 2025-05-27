import psycopg2
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Load environment variables from .env file
load_dotenv()



# Function to create a psycopg2 connection to the data warehouse (DWH)
def repstat_psycopg2_connection(host, db):
    # Read environment variables for DWH connection parameters
    conn = psycopg2.connect(
        host=host,
        database=db,
        user=os.environ.get('REPLICA_USER'),
        password=os.environ.get('REPLICA_PASSWORD'))
    # Return the psycopg2 connection object
    return conn


# Function to create a psycopg2 connection to the data warehouse (DWH)
def create_dwh_psycopg2_connection():
    # Read environment variables for DWH connection parameters
    conn = psycopg2.connect(
        host=os.environ.get('DWH_HOST'),
        database=os.environ.get('DWH_DB'),
        user=os.environ.get('DWH_USER'),
        password=os.environ.get('DWH_PASSWORD'))
    # Return the psycopg2 connection object
    return conn


# Function to create a SQLAlchemy engine for connecting to the DWH
def create_dwh_sqlalchemy_engine():
    # Read environment variables for DWH connection parameters
    user = os.environ.get('DWH_USER')
    password = os.environ.get('DWH_PASSWORD')
    host = os.environ.get('DWH_HOST')
    dbname = os.environ.get('DWH_DB')
    
    # Create a SQLAlchemy engine using the psycopg2 connector
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}")

# Create a SQLAlchemy session factory using the DWH engine
Session = sessionmaker(bind=create_dwh_sqlalchemy_engine())


def save_to_dwh(df, table_name, schema):
    
    # Save the dataframe to the specified table in the DWH
    df.to_sql(
        table_name,
        con=create_dwh_sqlalchemy_engine(),
        schema=schema,
        if_exists='append',
        index=False,
        chunksize=10000
    )


def truncate_dwh_table(schema, table_name):
    
    # Create a psycopg2 connection to the DWH
    conn = create_dwh_psycopg2_connection()
    cur = conn.cursor()
    
    # Execute a TRUNCATE TABLE SQL statement
    cur.execute(f"TRUNCATE TABLE {schema}.{table_name}")
    
    # Commit the changes and close the cursor and connection
    conn.commit()

    cur.close()
    conn.close()
