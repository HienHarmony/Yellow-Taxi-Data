import pandas as pd
import json
from sqlalchemy import create_engine
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    path = params.path
    table = params.table
    
    # Connect to the PostgreSQL database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    try: 
        # Read the Parquet file into a pandas DataFrame
        data = pd.read_parquet(path)
        print("Read data successfully!")
        
        # Write the DataFrame to the PostgreSQL table without dropping it
        data.to_sql(name=table, con=engine, if_exists="append", index=False)
        print("Data inserted into PostgreSQL table successfully!")
        
    except Exception as e:
        print(f"Failed to insert data into PostgreSQL table: {e}")
        raise

if __name__ == '__main__':
    # Define command-line arguments
    params = argparse.ArgumentParser(description="Ingest Parquet file into PostgreSQL table")
    params.add_argument('--user', help="username for PostgreSQL", default="vanhien")
    params.add_argument('--password', help="password for PostgreSQL user", default="1234")
    params.add_argument('--host', help="host of PostgreSQL server", default="localhost")
    params.add_argument('--port', help="port of PostgreSQL server", default="5432")
    params.add_argument('--db', help="name of the PostgreSQL database", default="taxi_data")
    params.add_argument('--table', help="name of the table to insert data into", default="data")
    params.add_argument('--path', help="path to the Parquet file", default="/home/hiennv/code/data/yellow_data.parquet")
    
    # Parse command-line arguments
    args = params.parse_args()
    
    # Call the main function with parsed arguments
    main(args)

