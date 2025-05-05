from google.cloud import storage
import json
from google.cloud import bigquery
import pandas as pd
import logging
import os

CREDENTIALS = "/.googlecloud/credentials.json"

def read_files(file_dir : str) :
    """
        Helper function to read a dictionary of filenames
        into a combined dataframe

        Args:
            file_dir : the file_dir where the files are stored
        
        Returns :
            df : the combined dataframe
    """
    df = pd.DataFrame()
    for file_name in os.listdir(file_dir):
        file_path = os.path.join(file_dir, file_name)
        if os.path.isfile(file_path):
            temp = pd.DataFrame()
            if file_path.endswith(".csv") :
                temp = pd.read_csv(file_path)
            elif file_path.endswith(".json") :
                temp = pd.read_json(file_path)
                
            df = pd.concat([df,temp])
    
    return df

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket. 
    Taken from : https://cloud.google.com/storage/docs/ 
    
    Args :
        - bucket_name : (String) the name of the bucket
        - source_file_name : (String) the filename from local directory
        - destination_blob_mame : (String) the filename to be saved as        
    
    """
    client = storage.Client.from_service_account_json(
        CREDENTIALS
    )
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    # Check if the blob already exists
    if blob.exists():
        logging.info('Blob {} already exists. Skipping upload.'.format(destination_blob_name))
        return
    
    # Upload the file
    blob.upload_from_filename(source_file_name)
    logging.info('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def load_to_bq(df, dataset_id, table_id):
    """Loads transformed data from workflows into bigquery

    Args :
      - df : (pd.DataFrame) - the dataframe after transformation
      - dataset_id : (String) - id ref of the dataset in bigquery
      - table_id : (String) - id ref of the table in bigquery

    Adds the json data onto the existing bigquery table
    """

    client = bigquery.Client.from_service_account_json(CREDENTIALS)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True

    load_job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config)

    load_job.result()

    print("Data loaded to {}:{}.".format(dataset_id, table_id))


def read_from_bq(dataset_id, table_id) -> pd.DataFrame:
    """Read table data from bigquery into a pandas dataframe

    Args :
      - dataset_id : (String) - id ref of dataset in bigquery
      - table_id : (String) - id ref of table in bigquery

    Returns :
      - pd.DataFrame : the table data in pd.DataFrame
    """
    client = bigquery.Client.from_service_account_json(CREDENTIALS)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    df = client.list_rows(table).to_dataframe()

    return df

def move_blob(bucket_name, source_blob_name, destination_blob_name):
    """Move a blob from one location to another within the same bucket.
    
    Args : 
        - bucket_name : the bucket name where we store our files
        - source_blob_name : the filepath of the source blob 
        - dest_blob_name : the filepath of the dest blob
    """
    # Initialize GCS client
    client = storage.Client.from_service_account_json(
    CREDENTIALS)  # initialize gcs client

    # Get source bucket and blob
    source_bucket = client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(source_blob_name)

    # Get destination bucket and blob
    destination_bucket = client.get_bucket(bucket_name)
    destination_blob = destination_bucket.blob(destination_blob_name)

    # Delete previous file if exists
    if destination_blob.exists():
        logging.info(f"File '{destination_blob_name}' already exists. Deleting existing file.")
        destination_blob.delete()

    # Copy blob to the new location
    source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

    # Delete the original blob
    source_blob.delete()
