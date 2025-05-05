import json
import io
from datetime import datetime, timedelta
from helpers.stores.utils import load_to_bq, upload_blob
from helpers.utils import cleanup
from helpers.transformation.general import (
    transform_stage_1,
    transform_stage_2,
    transform_stage_3,
)

from airflow.decorators import dag, task, task_group

import pandas as pd
from constants import (
    BUCKET_NAME,
    JSON_DIR,
    QUERY_CSV,
    SESSION_CSV,
    CREDENTIALS,
    DATASET_ID,
    TABLE_ID,
)
from google.cloud import storage


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 10,
    "retry_delay": timedelta(minutes=2),
}

client = storage.Client.from_service_account_json(
    CREDENTIALS)  # initialize gcs client

bucket = client.get_bucket(BUCKET_NAME)


@dag(
    dag_id="backup_sicpama",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["data_lake"],
)
def backup_etl_pipeline():
    @task(task_id="start_etl")
    def start():
        pass

    @task(task_id="end_etl")
    def end():
        pass

    @task(task_id="extract_data")
    def extract_data_to_gcs():
        """
        TODO: Implement your extraction logic here.
        In our current case, since data is presumed to
        already be loaded into gcs buckets,
        there is no data to be extracted
        src_file_name = PATH TO YOUR SRC FILES FOR THE GA JSON AND SQL FILES
        NOTE: You can implement your extraction of csv from sql here
        """

        ## load to gcs data lake
        ## 
        ## NOTE : We do not have access so we assumed
        ## fetch files from wherever and store it into the GCS Bucket
        ## upload_blob(BUCKET_NAME, src_file_name=src_file_name, destination_blob_name=dst_blob_name)
        pass

    # TODO : This portion can be optimized alot
    # If we can divide the session and query dataframes by dates,
    # would be great. We can then use the dates to partition the chunks
    @task(task_id="transform_data")
    def transform():
        """
        Pull relevant data from GCS storage 
        and apply transformations for the final data
        The relevant files include :
            - session files
            - query files
            - ga files
        RECOMMENDATION 1 : 
        - Use GCP dataflow if you want to keep it all in 1 service provider
        RECOMMENDATION 2 : 
        - Transform using distributed processing instruments like DASK instead of pandas
        RECOMMENDATION 3 : 
        - Use a data friendly format like parquet or avro instead of json
        """
        # blobs = bucket.list_blobs(prefix=JSON_DIR)
        tasks = []
        session_blobs = bucket.list_blobs(prefix=SESSION_CSV)
        query_blobs = bucket.list_blobs(prefix=QUERY_CSV)
        ga_blobs = bucket.list_blobs(prefix=JSON_DIR)

        session_df = pd.DataFrame()
        query_df = pd.DataFrame()

        for blob in session_blobs:
            blob_content = blob.download_as_string()
            df = pd.read_csv(io.BytesIO(blob_content))
            session_df = pd.concat([session_df, df], ignore_index=True)

        for blob in query_blobs:
            blob_content = blob.download_as_string()
            df = pd.read_csv(io.BytesIO(blob_content))
            query_df = pd.concat([query_df, df], ignore_index=True)

        # array of df
        for blob in ga_blobs:
            if blob.name.startswith(JSON_DIR + "/SicPama Google Analytics dataset"):
                # Download the blob to a file
                try:
                    filepath = "/tmp/blob.json"
                    blob.download_to_filename(filepath)
                    print("Download success")
                    with open(filepath, "r") as f:
                        json_dict = json.load(f)
                        cols = json_dict["columns"]
                        data = json_dict["data"]

                        # Define chunk size
                        chunk_size = 100000
                        
                        # Process data in chunks of size 10000
                        while data:
                            # Extract a chunk of data of size 10000 rows
                            chunk_data = data[:chunk_size]
                            
                            data = data[chunk_size:]
                            # Create a DataFrame from the chunk of data
                            df = pd.DataFrame(chunk_data, columns=cols)
                            print(df.head())
                            print("Read sucesss")
                            print("-------- Transforming stage 1 --------")
                            df_stage1 = transform_stage_1(df)

                            cleanup(df)  # remove df dataframe

                            df_stage2 = transform_stage_2(df_stage1)

                            cleanup(df_stage1) # remove dataframe for memory

                            df_stage3 = transform_stage_3(
                                ga_df=df_stage2,
                                order_df=query_df,
                                sessions_df=session_df,
                            )

                            cleanup(df_stage2)

                            load_to_bq(df_stage3, DATASET_ID, TABLE_ID)

                            cleanup(df_stage3)

                    blob.delete()
                except Exception as e:
                    print(e)
        return tasks

    @task(task_id="load_data")
    def load():
        """
        TODO: Implement your loading logic here
                            )
        """
        pass

    start() >> extract_data_to_gcs() >> transform() >> load() >> end()


backup_etl_pipe = backup_etl_pipeline()
