import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime
import pytz

PROJECT_TIMEZONE = pytz.timezone('Asia/Jerusalem')


def get_current_timestamp():
    return datetime.now(PROJECT_TIMEZONE)


def get_table_location(client, project_id, dataset_id, table_name):
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    try:
        table = client.get_table(table_id)
        return table.location
    except NotFound:
        logging.error(f"Table {table_id} not found.")
        return None


def table_exists(client, project_id, dataset_id, table_name):
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def create_ai_table(client, project_id, dataset_id, table_name):
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

    create_bigquery_table(client, project_id, dataset_id, table_name, schema)


def create_bigquery_table(client, project_id, dataset_id, table_name, schema):
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)
    try:
        table = client.create_table(table, exists_ok=True)
        logging.info(f"Table {table.project}.{table.dataset_id}.{table.table_id} is ready.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise


def insert_rows_to_bigquery(client, project_id, dataset_id, table_name, rows):
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    if not table_exists(client, project_id, dataset_id, table_name):
        logging.warning(f"Table {table_id} not found. Attempting to create it.")
        create_ai_table(client, project_id, dataset_id, table_name)

    current_time = get_current_timestamp()
    for row in rows:
        row['created_at'] = current_time.isoformat()

    try:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            logging.error(f"Errors inserting rows: {errors}")
        else:
            logging.info(f"Inserted {len(rows)} rows into {table_id}")
    except Exception as e:
        logging.error(f"Error inserting rows: {e}")
        raise