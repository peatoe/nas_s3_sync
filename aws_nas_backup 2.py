import os
import boto3
from urllib.parse import urlparse
from tqdm import tqdm
from datetime import datetime, timezone
import logging

# Configure logging to write to a file
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='xxx/xxx/xxx/xxx',  # Log messages are written to this file
                    filemode='a')  # 'a' for append mode

def get_bucket_and_key(uri):
    parsed_uri = urlparse(uri)
    return parsed_uri.netloc, parsed_uri.path.lstrip('/')

def list_local_files(local_path):
    all_files = []
    for root, dirs, files in os.walk(local_path):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for file in files:
            if not file.startswith('.'):  # Skip hidden files
                file_path = os.path.join(root, file)
                all_files.append(file_path)
    return all_files

def list_s3_files(s3_client, bucket, prefix=''):
    s3_objects = []
    continuation_token = None
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        s3_objects.extend(response.get('Contents', []))
        
        if response.get('IsTruncated'):  # True if more results are available
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    return s3_objects

def get_object_age(last_modified):
    current_time = datetime.now(timezone.utc)
    age = current_time - last_modified
    return age.days

def upload_to_s3(s3_client, local_file, bucket, s3_key, storage_class, uploaded_files):
    try:
        filesize = os.path.getsize(local_file) / (1024 ** 2)  # Convert to MB
        description = f'Uploading: {os.path.basename(local_file)} ({filesize:.2f} MB)'
        with tqdm(total=filesize * 1024 * 1024, unit='B', unit_scale=True, desc=description) as pbar:
            def update_progress(chunk):
                pbar.update(chunk)

            extra_args = {'StorageClass': storage_class}
            s3_client.upload_file(local_file, bucket, s3_key, ExtraArgs=extra_args, Callback=update_progress)
        uploaded_files.append(s3_key)
        logging.info(f"Successfully uploaded {local_file} to {s3_key}")
    except Exception as e:
        logging.error(f"Error uploading {local_file}: {str(e)}")

def delete_from_s3(s3_client, bucket, s3_key, last_modified, deleted_files, min_age_days=180):
    try:
        object_age = get_object_age(last_modified)

        if object_age >= min_age_days:
            s3_client.delete_object(Bucket=bucket, Key=s3_key)
            deleted_files.append(s3_key)
            logging.info(f"Deleted {s3_key} from S3")
        else:
            logging.info(f"Skipped deletion of {s3_key}; it is less than {min_age_days} days old.")
    except Exception as e:
        logging.error(f"Error deleting {s3_key}: {str(e)}")

def sync_local_to_s3(local_path, s3_uri, storage_class='DEEP_ARCHIVE', min_age_days=180):
    try:
        s3_client = boto3.client('s3')
        bucket, prefix = get_bucket_and_key(s3_uri)
        local_files = list_local_files(local_path)
        s3_files = list_s3_files(s3_client, bucket, prefix)

        # Map S3 keys to their last modified times
        s3_files_last_modified = {s3_obj['Key']: s3_obj['LastModified'] for s3_obj in s3_files}

        uploaded_files = []
        deleted_files = []

        # Upload new or updated files
        for local_file in local_files:
            relative_path = os.path.relpath(local_file, local_path)
            s3_key = os.path.join(prefix, relative_path).replace("\\", "/")

            if s3_key not in s3_files_last_modified:
                upload_to_s3(s3_client, local_file, bucket, s3_key, storage_class, uploaded_files)
            else:
                local_file_modified_time = datetime.fromtimestamp(os.path.getmtime(local_file), timezone.utc)
                if local_file_modified_time > s3_files_last_modified[s3_key]:
                    upload_to_s3(s3_client, local_file, bucket, s3_key, storage_class, uploaded_files)

        # Delete files from S3 that no longer exist locally
        for s3_file in s3_files:
            local_file = os.path.join(local_path, os.path.relpath(s3_file['Key'], prefix))
            if not os.path.exists(local_file):
                delete_from_s3(s3_client, bucket, s3_file['Key'], s3_files_last_modified[s3_file['Key']], deleted_files, min_age_days)

        # Logging uploaded and deleted files
        if uploaded_files:
            logging.info("Uploaded Files:")
            for file in uploaded_files:
                logging.info(file)
        else:
            logging.info("No files to upload.")

        if deleted_files:
            logging.info("Deleted Files:")
            for file in deleted_files:
                logging.info(file)
        else:
            logging.info("No files to delete.")
    except Exception as e:
        logging.error(f"Error during sync: {str(e)}")

if __name__ == "__main__":
    logging.info("Starting the S3 sync process")
    s3_uri = 's3://xxx'
    local_path = '/xxx/xxx/xxx'
    storage_class = 'DEEP_ARCHIVE' # Choose storage class to upload to
    min_age_days = 180 # Use in order to not delete files less than x ammount of days old from s3. Comes in handy for using Glacier were you are charged for early deletion of files

    sync_local_to_s3(local_path, s3_uri, storage_class=storage_class, min_age_days=min_age_days)
