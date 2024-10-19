from google.cloud import storage
import logging


def list_gcs_folders(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter='/')
    prefixes = set()
    for page in blobs.pages:
        prefixes.update(page.prefixes)
    return sorted([prefix.split('/')[-2] for prefix in prefixes if prefix.count('/') > 1], reverse=True)


def list_gcs_files(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.lower().endswith('.mp4')]


def get_latest_folder_and_files(bucket_name, base_prefix):
    folders = list_gcs_folders(bucket_name, base_prefix)
    if not folders:
        logging.error("No folders found.")
        return None, []

    latest_folder = folders[0]
    prefix = f"{base_prefix}{latest_folder}/"
    video_files = list_gcs_files(bucket_name, prefix)

    if not video_files:
        logging.error(f"No video files found in the latest folder: {latest_folder}")
        return latest_folder, []

    return latest_folder, video_files