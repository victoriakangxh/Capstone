import time
from datetime import datetime, timedelta

import os
import typing
import re
import logging
import json
import pandas as pd

from airflow.decorators import dag, task, task_group
from google.cloud import storage

from constants import (
    BUCKET_NAME,
    GA_JSON,
    QUERY_CSV,
    SESSION_CSV,
    CREDENTIALS,
    DATASET_ID,
    TABLE_ID,
    DATA_DIR
)

from helpers.stores.utils import load_to_bq, move_blob, read_files
from helpers.utils import cleanup
from helpers.transformation.general import (
    transform_stage_1,
    transform_stage_2,
    transform_stage_3,
)



default_args = {
    "owner": "sicpama",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

client = storage.Client.from_service_account_json(
    CREDENTIALS)  # initialize gcs client

bucket = client.get_bucket(BUCKET_NAME)

@dag(
    dag_id="sicpama_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["etl","sicpama"],
)

def etl_pipeline():
    """
        Author: James Poh Hao
        Co-Author : Victoria Kang
        Date: 12-04-2024

        ETL Pipeline
        orchestrates the Extract, Transform, Load (ETL) process for data integration and analysis. 
        It begins by extracting data stored in JSON, CSV, and SQL formats from Google Cloud Storage (GCS), 
        leveraging cloud-based storage capabilities for efficient data retrieval.
        Subsequently, the extracted data undergoes transformation through user-defined operations 
        executed on local computational resources. These transformations enable data cleansing, enrichment, 
        aggregation, and other processing steps tailored to specific analytical requirements.
        Upon completion of the transformation phase, the cleaned and processed data is loaded into Google BigQuery, 
        a scalable and high-performance data warehouse service. BigQuery's robust infrastructure facilitates storage, 
        querying, and analysis of large datasets, empowering data-driven decision-making and insights generation.
        This ETL pipeline encapsulates best practices in data engineering, ensuring the seamless integration, transformation,
        and loading of diverse data sources into a centralized repository for advanced analytics and reporting.

    """
    #region : Dummy/Helper operators
    @task(task_id="start_task")
    def start():
        """
            Dummy operator to initiate a task group or pipeline
        """
        return "Done" #
    
    @task(task_id = "await", trigger_rule = "all_done")
    def await_tasks(t : int) :
        """
            A sleep operator to wait for {t} amount of seconds

            :t: the number of seconds to wait for

            Returns :
                Delay of t seconds
        """
        time.sleep(t)

    @task(task_id="end_task")
    def end():
        """
            An end operator to signal the end of the group task or pipeline
        """
        logging.info("---------- END GROUP OR DAG TASKS ----------")
        return "Done"
    
    @task(task_id = "make_dir")
    def make_dir(ga_dir, session_dir, query_dir) :
        """
            Make local directories not exist
            :ga_dir: the directory for ga files
            :session_dir: the directory for session files
            :query_dir: the directory for query files
        """
        os.makedirs(ga_dir, exist_ok= True)
        os.makedirs(session_dir, exist_ok= True)
        os.makedirs(query_dir, exist_ok= True)

    #endregion

    #region : Extract
    @task(task_id="extract_data")
    def extract_data(prefix: str, dest_dir : str):
        """
        Extract data from gcs and store
        them locally

        :prefix: the prefix of the files to look for
        :dest_dir: the directory of our local files

        """
        logging.info(f"--------- DOWNLOADING FILES : {prefix} ---------")
        blobs = bucket.list_blobs(prefix=prefix)

        for blob in blobs:
            if not blob.name.endswith("/"): # we only want to download valid files
                dest_path = f"{dest_dir}{blob.name.split('/')[-1]}"
                blob.download_to_filename(dest_path)
        

    @task_group(group_id="extract_group_data")
    def group_extract_data_from_gcs(ga_dir, session_dir, query_dir):
        """
        TODO: Implement other extraction logic related to sql here.
        In our current case, since data is presumed to
        already be loaded into gcs buckets,
        there is no sql data to be extracted
        NOTE: You can implement your extraction of csv from sql here
        """
        query_id = QUERY_CSV.split("/")[-1] # query task id
        session_id = SESSION_CSV.split("/")[-1] # session task id
        ga_id = GA_JSON.split("/")[0] # ga task id
        
        ### Initialize download of each data from gcs ### 
        logging.info("------- WAITING FOR FILES DOWNLOAD ------ ")
        download_file_session = extract_data.override(
            task_id=f"extract_data_{session_id}"
        )(
            SESSION_CSV,
            dest_dir = session_dir
        )
        download_file_query = extract_data.override(
            task_id=f"extract_data_{query_id}"
        )(
            QUERY_CSV,
            dest_dir = query_dir
        )

        download_file_ga = extract_data.override(
            task_id=f"extract_data_{ga_id}")(
            GA_JSON,
            dest_dir = ga_dir
        )
        logging.info("------- END OF FILES DOWNLOAD ------ ")
        
        start() >> [ download_file_ga, download_file_query, download_file_session ] >> end()
        
    #endregion

    #region : Transform
    @task(task_id = "transform_stage_1")
    def transform_stage1(filepath : str) :
        """
            Task operator for stage 1 transformation.
            Reads the json file and saves the newly transformed
            json file back into our local directory

            :filepath: the string filepath of our json file
            
        """
        logging.info("--------- OPENING FILE ---------")
        with open(filepath, "r") as f: # First stage requires to open the file due to the way the file was saved 
            json_dict = json.load(f)
            if json_dict : # Check for existence of json dict
                columns = json_dict["columns"]
                print(columns)
                logging.info("--------- READING FILE  ---------")
                ga_df = pd.DataFrame(json_dict["data"], columns = columns)
                
                logging.info("--------- STARTING TRANSFORMATION STAGE 1 ---------")
                ga_df = transform_stage_1(ga_df)
                
                ga_df.reset_index(drop=True, inplace=True)
                
                logging.info("--------- FINISHED TRANSFORMATION STAGE 1 ---------")
                
                ga_df.to_json(filepath)
            else :
                logging.info("Json file is empty !!! Proceed to fail task !")
            
    
    @task(task_id = "transform_stage_2")
    def transform_stage2(filepath) :
        """
            Task operator for stage 2 transformation
            Reads the json file and saves the transformed
            json file back into our local directory

            :filepath: the string filepath of our json file
        """
        logging.info("--------- READING FILE  ---------")
        ga_df = pd.read_json(filepath)
        logging.info("--------- STARTING TRANSFORMATION STAGE 2 ---------")
        ga_df = transform_stage_2(ga_df)

        ga_df.reset_index(drop=True, inplace=True)
        logging.info("--------- FINISHED TRANSFORMATION STAGE 2 ---------")
        ga_df.to_json(filepath)
    
    @task(task_id = "transform_stage_3")
    def transform_stage3(session_dir, query_dir, filepath) :
        """
            Task operator for stage 3 transformation
            Reads the json file, combines with the session csv
            and the query csv and apply some transformation before
            saving it back into a json file

            :session_dir: the directory where we save the sessions csv files
            :query_dir: the directory where we save the query csv files
            :filepath: the string filepath of our json file

        """
        logging.info("--------- READING FILES  ---------")
        session_df = read_files(session_dir)
        query_df = read_files(query_dir)
        ga_df = pd.read_json(filepath)

        logging.info("--------- STARTING TRANSFORMATION STAGE 3 ---------")
        ga_df = transform_stage_3(ga_df=ga_df, order_df=query_df, sessions_df=session_df)

        ga_df.reset_index(drop=True, inplace=True)
        logging.info("--------- FINISHED TRANSFORMATION STAGE 3 ---------")
        ga_df.to_json(filepath)


    @task_group(group_id="transform_files")
    def transform_file(session_dir: str, query_dir: str, file_path: str, id: str):
        """
            A task group operator to encapsulate and organize the transformation

            :session_dir: the directory where we store the session_csvs
            :query_dir: the directory where we save the query csv files
            :filepath: the string filepath of our json file:
            :id: the unique id for each task

        """
        transform_1 = transform_stage1.override(task_id=f"Transforming_stage_1_{id}")(file_path)
        transform_2 = transform_stage2.override(task_id=f"Transforming_stage_2_{id}")(file_path)
        transform_3 = transform_stage3.override(task_id=f"Transforming_stage_3_{id}")(session_dir=session_dir, query_dir=query_dir, filepath=file_path)
        start() >> transform_1 >> transform_2 >> transform_3 >> end()
    #endregion

    #region : Load And Cleanup
    @task(task_id="load_ga_data")
    def load_all(ga_dir):
        """
            Load the transformed json file into bigquery

            :ga_dir: the directory where the files are stored

            Returns :
                Loads the data from our local directory into bigquery
        """
        for file_name in os.listdir(ga_dir):
            print(file_name)
            file_path = os.path.join(ga_dir, file_name)
            if os.path.isfile(file_path):
                df = pd.read_json(file_path)
                load_to_bq(df, DATASET_ID, TABLE_ID) # helper function to load into bq
                cleanup(df)
                #logging.info(file_path)
                os.remove(file_path) # delete the file after loading

    @task(task_id="move_to_past_dir")
    def move_files(prefix) :
        """
            Moves all files from the bucket directory
            into a past folder,
            can generalize it easily by adding a param for
            the temporary files to be stored
        """
        logging.info("--------- REBASING FILES ---------")
        blobs = bucket.list_blobs(prefix=prefix)

        for blob in blobs:
            if not blob.name.endswith("/"): # we only want to move valid files
                move_blob(BUCKET_NAME, blob.name, f"past/{blob.name.split('/')[-1]}")

    #endregion

    #region : Pipeline Workflow and Constants 
    ga_dir = f"{DATA_DIR}{GA_JSON}/"
    session_dir = f"{DATA_DIR}{SESSION_CSV}/"
    query_dir = f"{DATA_DIR}{QUERY_CSV}/"
        
    start_transform = start.override(task_id = "start_transform") ()
    end_transform = end.override(task_id = "end_transform") ()

    extracts = group_extract_data_from_gcs(ga_dir, session_dir, query_dir)

    blobs = bucket.list_blobs(prefix=GA_JSON)
    
    transforms = []
    for blob in blobs:
        if not blob.name.endswith("/"): # we only want to download valid files
            file_name = blob.name.split('/')[-1]
            id = re.sub(r'\W+', '', file_name)
            file_path = os.path.join(ga_dir, file_name)
            transform_idv = transform_file.override(group_id = f"transform_files_{id}") (session_dir, query_dir, file_path,id)
            transforms.append(transform_idv)
            # transforms.append(transform_idv)

    (start.override(task_id="start_etl")() >> 
    make_dir(ga_dir,session_dir,query_dir) >> 
    extracts >> 
    start_transform >> 
    transforms >> 
    end_transform >> 
    load_all(f"{DATA_DIR}{GA_JSON}") >> 
    move_files(GA_JSON) >> end.override(task_id="end_etl")())
    
    #endregion

pipeline = etl_pipeline()