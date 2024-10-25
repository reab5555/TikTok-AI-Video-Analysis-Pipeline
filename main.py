import time
import sys
import logging
import vertexai
from video_processor import generate
from gcs_utils import get_latest_folder_and_files
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from datetime import datetime
import pytz

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_TIMEZONE = pytz.timezone('Asia/Jerusalem')

def execute_query(client, query, description):
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    try:
        job = client.query(query, job_config=job_config)
        job.result()  # Wait for the job to complete
        logging.info(f"Successfully executed: {description}")
    except BadRequest as e:
        logging.error(f"Error in {description}: {e}")
        raise

def create_and_populate_tables(client, project_id, dataset_id, metadata_table_name, ai_results):
    current_time = datetime.now(PROJECT_TIMEZONE)
    current_date = current_time.date()
    current_timestamp = current_time.isoformat()

    # Create or use the permanent table for AI results
    ai_table_name = "ai_results"
    ai_table_id = f"{project_id}.{dataset_id}.{ai_table_name}"
    schema = [
        bigquery.SchemaField("video_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ai_unexpectedness_rating", "INTEGER"),  # text1
        bigquery.SchemaField("ai_unexpectedness_duration", "INTEGER"),  # text2
        bigquery.SchemaField("ai_expectation_violation_description", "STRING"),  # text3
        bigquery.SchemaField("ai_emotional_intensity", "INTEGER"),  # text4
        bigquery.SchemaField("ai_positivity", "INTEGER"),  # text5
        bigquery.SchemaField("ai_negativity", "INTEGER"),  # text6
        bigquery.SchemaField("ai_expected_desirability", "INTEGER"),  # text7
        bigquery.SchemaField("ai_unexpected_desirability", "INTEGER"),  # text8
        bigquery.SchemaField("ai_emotional_spatial_closeness", "INTEGER"),  # text9
        bigquery.SchemaField("ai_cognitive_interruption", "INTEGER"),  # text10
        bigquery.SchemaField("ai_perceived_realism", "INTEGER"),  # text11
        bigquery.SchemaField("ai_sexual_content_rating", "INTEGER")  # text12
    ]

    table = bigquery.Table(ai_table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)

    # Insert AI results into the AI results table
    errors = client.insert_rows_json(ai_table_id, ai_results)
    if errors:
        logging.error(f"Errors inserting AI results: {errors}")
        return

    # Create dim_lang table
    dim_lang_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_lang` AS
    SELECT ROW_NUMBER() OVER() as lang_id, lang, TIMESTAMP('{current_timestamp}') as created_at
    FROM (SELECT DISTINCT lang FROM `{project_id}.{dataset_id}.{metadata_table_name}`) t;
    """
    execute_query(client, dim_lang_query, "Create dim_lang table")

    # Create dim_user table
    dim_user_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_user` AS
    SELECT DISTINCT user_id, user, user_nickname, user_signature, user_followers, user_videos, TIMESTAMP('{current_timestamp}') as created_at
    FROM `{project_id}.{dataset_id}.{metadata_table_name}`;
    """
    execute_query(client, dim_user_query, "Create dim_user table")

    # Create dim_video table - Updated to use video_id instead of id
    dim_video_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_video` AS
    SELECT 
        CAST(m.id AS INT64) as video_id, 
        m.text, 
        m.gcs_path, 
        TIMESTAMP('{current_timestamp}') as created_at
    FROM `{project_id}.{dataset_id}.{metadata_table_name}` m
    JOIN `{ai_table_id}` a ON CAST(m.id AS INT64) = a.video_id;
    """
    execute_query(client, dim_video_query, "Create dim_video table")

    # Create fact_video_analytics table - Updated to use video_id
    fact_table_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.fact_video_analytics` AS
    SELECT 
        CAST(m.id AS INT64) as video_id,
        m.user_id,
        l.lang_id,
        m.createTimeISO,
        m.duration,
        m.video_likes,
        m.video_shares,
        m.video_plays,
        m.video_bookmarks,
        m.video_comments,
        a.ai_unexpectedness_rating,
        a.ai_unexpectedness_duration,
        a.ai_expectation_violation_description,
        a.ai_emotional_intensity,
        a.ai_positivity,
        a.ai_negativity,
        a.ai_expected_desirability,
        a.ai_unexpected_desirability,
        a.ai_emotional_spatial_closeness,
        a.ai_cognitive_interruption,
        a.ai_perceived_realism,
        a.ai_sexual_content_rating,
        DATE('{current_date}') as analysis_date,
        TIMESTAMP('{current_timestamp}') as created_at
    FROM `{project_id}.{dataset_id}.{metadata_table_name}` m
    JOIN `{ai_table_id}` a ON CAST(m.id AS INT64) = a.video_id
    JOIN `{project_id}.{dataset_id}.dim_lang` l ON m.lang = l.lang;
    """
    execute_query(client, fact_table_query, "Create fact_video_analytics table")

    logging.info(f"Star schema populated with data from {metadata_table_name} and AI results")

if __name__ == "__main__":
    # Initialize VertexAI
    vertexai.init(project="python-code-running", location="me-west1")

    # Set up BigQuery client
    project_id = "python-code-running"
    bq_client = bigquery.Client(project=project_id)

    # List all video files in the bucket from the latest date folder
    bucket_name = "main_il"
    base_prefix = "TIKTOK_samples/"
    latest_folder, video_files = get_latest_folder_and_files(bucket_name, base_prefix)

    if not latest_folder:
        logging.error("No folders found. Exiting.")
        sys.exit(1)

    if not video_files:
        logging.error("No video files found. Exiting.")
        sys.exit(1)

    logging.info(f"Processing videos from folder: {latest_folder}")
    logging.info(f"Processing the following videos: {', '.join(video_files)}")

    # Start the timer
    start_time = time.time()

    # Define LLM configuration
    config = {
        "temperature": 0.5,
        "top_p": 0.95
    }

    # Run generate function
    logging.info(f"Running analysis with configuration: {config}")
    all_results = generate(video_files, bucket_name,
                           temperature=config["temperature"],
                           top_p=config["top_p"])

    # End the timer
    end_time = time.time()

    # Print the total execution time
    logging.info(f"\nTotal execution time: {end_time - start_time:.2f} seconds")

    # Create and populate BigQuery tables
    if all_results:
        dataset_id = "tiktok_data"
        metadata_table_name = "tiktok_videos_metadata"
        create_and_populate_tables(bq_client, project_id, dataset_id, metadata_table_name, all_results)
    else:
        logging.warning("No AI results generated. BigQuery tables were not created or populated.")

    logging.info(f"Script execution completed at {datetime.now(PROJECT_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S %Z')}")
