import logging
import json
import sys
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from datetime import datetime
import pytz
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_TIMEZONE = pytz.timezone('Asia/Jerusalem')


def get_latest_ai_results_file():
    files = [f for f in os.listdir('.') if f.startswith('ai_results_') and f.endswith('.json')]
    if not files:
        raise FileNotFoundError("No AI results file found.")
    return max(files)


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


def join_and_populate_star_schema(client, project_id, dataset_id, metadata_table_name, ai_results):
    current_time = datetime.now(PROJECT_TIMEZONE)
    current_date = current_time.date()
    current_timestamp = current_time.isoformat()

    # Create a temporary table for AI results
    ai_temp_table = f"{project_id}.{dataset_id}.temp_ai_results"
    schema = [
        bigquery.SchemaField("video_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("unexpectedness_rating", "INTEGER"),
        bigquery.SchemaField("emotional_intensity", "INTEGER"),
        bigquery.SchemaField("timecode", "STRING"),
        bigquery.SchemaField("expectation_description", "STRING"),
        bigquery.SchemaField("violation_description", "STRING"),
        bigquery.SchemaField("expectation_probability", "FLOAT"),
        bigquery.SchemaField("sexual_content_rating", "INTEGER")
    ]

    table = bigquery.Table(ai_temp_table, schema=schema)
    table = client.create_table(table, exists_ok=True)

    # Insert AI results into the temporary table
    errors = client.insert_rows_json(ai_temp_table, ai_results)
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

    # Create dim_video table
    dim_video_query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.dim_video` AS
    SELECT 
        CAST(m.id AS INT64) as video_id, 
        m.text, 
        m.gcs_path, 
        a.timecode, 
        a.expectation_description, 
        a.violation_description,
        TIMESTAMP('{current_timestamp}') as created_at
    FROM `{project_id}.{dataset_id}.{metadata_table_name}` m
    JOIN `{ai_temp_table}` a ON CAST(m.id AS STRING) = a.video_id;
    """
    execute_query(client, dim_video_query, "Create dim_video table")

    # Create fact_video_analytics table
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
        a.unexpectedness_rating,
        a.emotional_intensity,
        a.expectation_probability,
        a.sexual_content_rating,
        DATE('{current_date}') as analysis_date,
        TIMESTAMP('{current_timestamp}') as created_at
    FROM `{project_id}.{dataset_id}.{metadata_table_name}` m
    JOIN `{ai_temp_table}` a ON CAST(m.id AS STRING) = a.video_id
    JOIN `{project_id}.{dataset_id}.dim_lang` l ON m.lang = l.lang;
    """
    execute_query(client, fact_table_query, "Create fact_video_analytics table")

    # Drop the temporary AI results table
    drop_temp_table_query = f"DROP TABLE `{ai_temp_table}`;"
    execute_query(client, drop_temp_table_query, "Drop temporary AI results table")

    logging.info(f"Star schema populated with data from {metadata_table_name} and AI results")

