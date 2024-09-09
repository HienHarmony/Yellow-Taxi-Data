import os 
import argparse


def get_data(args):
    """
    This function gets the data from the source and saves it in the folder

    Parameters
    ----------
    args : argparse.Namespace
        Arguments passed to the script

    Returns
    -------
    None
    """
    # Get the data from the source
    print("Downloading data...")
    os.system(f"curl -o {args.output_filepath} {args.url}")
    print("Data downloaded successfully")

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Get data from source")
    parser.add_argument("--url", default="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet", help="URL to the source data")
    parser.add_argument("--output_filepath", default="/home/hiennv/code/data/yellow_data.parquet", help="Filepath to save the data")
    args = parser.parse_args()
    get_data(args)
