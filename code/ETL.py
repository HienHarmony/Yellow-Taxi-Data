import pandas as pd
from sqlalchemy import create_engine
import argparse

def extract_data(path):
    "
    Extract data from a Parquet file into a pandas DataFrame.
    
    Parameters
    ----------
    path : str
        Path to the Parquet file.
        
    Returns
    -------
    pd.DataFrame
        Extracted data as a DataFrame.
    "
    try:
        data = pd.read_parquet(path)
        return data
    except Exception as e:
        print(f"Failed to extract data: {e}")
        raise

def transform_data(data):
    "
    Transform the data. This is a placeholder for any data transformation you need.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data to be transformed.
        
    Returns
    -------
    pd.DataFrame
        Transformed data.
    "
    # Example transformation: Drop duplicates
    data = data.drop_duplicates()
    
    # Additional transformation logic can be added here
    # For example, handling missing values, converting data types, etc.
    
    return data

def load_data(data, user, password, host, port, db, table):
    "
    Load the data into a PostgreSQL table.
    
    Parameters
    ----------
    data : pd.DataFrame
        Data to be loaded.
    user : str
        Username for PostgreSQL.
    password : str
        Password for PostgreSQL.
    host : str
        Host of PostgreSQL server.
    port : str
        Port of PostgreSQL server.
    db : str
        Database name.
    table : str
        Table name.
        
    Returns
    -------
    None
    "
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        data.to_sql(name=table, con=engine, if_exists="append", index=False)
    except Exception as e:
        print(f"Failed to load data into PostgreSQL table: {e}")
        raise

def main(params):
    # Extract
    data = extract_data(params.path)
    
    # Transform
    transformed_data = transform_data(data)
    
    # Load
    load_data(transformed_data, params.user, params.password, params.host, params.port, params.db, params.table)

if __name__ == '__main__':
    # Define command-line arguments
    parser = argparse.ArgumentParser(description="ETL process for data from Parquet to PostgreSQL")
    parser.add_argument('--user', help="Username for PostgreSQL", default="vanhien")
    parser.add_argument('--password', help="Password for PostgreSQL user", default="1234")
    parser.add_argument('--host', help="Host of PostgreSQL server", default="localhost")
    parser.add_argument('--port', help="Port of PostgreSQL server", default="5432")
    parser.add_argument('--db', help="Name of the PostgreSQL database", default="taxi_data")
    parser.add_argument('--table', help="Name of the table to insert data into", default="data")
    parser.add_argument('--path', help="Path to the Parquet file", default="/home/hiennv/code/data/yellow_data.parquet")
    
    # Parse command-line arguments
    args = parser.parse_args()
    
    # Call the main function with parsed arguments
    main(args)
