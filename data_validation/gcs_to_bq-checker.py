import re
import logging
import time
from google.cloud import storage
from google.cloud import bigquery

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

TARGET_BUCKET = 'z316-tiny-api'
STAGING_BUCKET = 'z316-staging'
TARGET_PDV_TABLE = 'emporio-zingaro.z316_tiny_raw_json.pdv'
TARGET_PESQUISA_TABLE = 'emporio-zingaro.z316_tiny_raw_json.pesquisa'
FIX = False

def list_gcs_folders(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = client.list_blobs(bucket)
    ids = set()
    folder_paths = []
    for blob in blobs:
        match = re.match(r'(\d+T\d+)-(\d+)-([-\w]+)', blob.name)
        if match:
            ids.add(match.group(2))
            folder_paths.append(blob.name)
            logging.debug(f"Found ID: {match.group(2)} from {blob.name}")
    return ids, folder_paths

def fetch_sale_ids_from_bigquery(table_name):
    client = bigquery.Client()
    query = f"SELECT id FROM `{table_name}`"
    df = client.query(query).to_dataframe()

    logging.debug(f"Fetched IDs from {table_name} into DataFrame")
    return set(df['id'].astype(str))

def find_missing_ids(gcs_ids, pdv_ids, pesquisa_ids):
    missing_in_pdv = pdv_ids - gcs_ids
    missing_in_pesquisa = pesquisa_ids - gcs_ids
    missing_in_both_tables = gcs_ids - (pdv_ids | pesquisa_ids)
    present_in_both_tables = (pdv_ids & pesquisa_ids) - gcs_ids
    missing_in_gcs_from_pdv = pdv_ids - gcs_ids
    missing_in_gcs_from_pesquisa = pesquisa_ids - gcs_ids
    unmatched_in_pdv = pdv_ids - pesquisa_ids - gcs_ids
    unmatched_in_pesquisa = pesquisa_ids - pdv_ids - gcs_ids
    present_in_gcs_not_in_pdv = gcs_ids - pdv_ids
    present_in_gcs_not_in_pesquisa = gcs_ids - pesquisa_ids

    logging.info(f"IDs in PDV not in GCS: {missing_in_pdv}")
    logging.info(f"IDs in Pesquisa not in GCS: {missing_in_pesquisa}")
    logging.info(f"IDs missing in both PDV and Pesquisa but present in GCS: {missing_in_both_tables}")
    logging.info(f"IDs present in both PDV and Pesquisa but not in GCS: {present_in_both_tables}")
    logging.info(f"IDs missing in GCS but present in PDV: {missing_in_gcs_from_pdv}")
    logging.info(f"IDs missing in GCS but present in Pesquisa: {missing_in_gcs_from_pesquisa}")
    logging.info(f"Unmatched IDs in PDV (not in GCS or Pesquisa): {unmatched_in_pdv}")
    logging.info(f"Unmatched IDs in Pesquisa (not in GCS or PDV): {unmatched_in_pesquisa}")
    logging.info(f"IDs present in GCS but not in PDV: {present_in_gcs_not_in_pdv}")
    logging.info(f"IDs present in GCS but not in Pesquisa: {present_in_gcs_not_in_pesquisa}")

    return missing_in_both_tables

def move_folders_for_triggering(bucket_name, staging_bucket_name, folder_paths):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    staging_bucket = storage_client.bucket(staging_bucket_name)

    for folder_path in folder_paths:
        source_blob = bucket.blob(folder_path)
        staging_blob = staging_bucket.blob(folder_path)
        staging_bucket.copy_blob(source_blob, staging_bucket, new_name=folder_path)
        logging.debug(f"Copied {folder_path} to staging bucket")
        source_blob.delete()
        logging.debug(f"Deleted {folder_path} from target bucket")

    for folder_path in folder_paths:
        staging_blob = staging_bucket.blob(folder_path)
        bucket.copy_blob(staging_blob, bucket, new_name=folder_path)
        logging.debug(f"Copied {folder_path} back to target bucket after triggering")
        time.sleep(10)

def main():
    logging.info("Starting the script...")

    gcs_ids, folder_paths = list_gcs_folders(TARGET_BUCKET)
    pdv_ids = fetch_sale_ids_from_bigquery(TARGET_PDV_TABLE)
    pesquisa_ids = fetch_sale_ids_from_bigquery(TARGET_PESQUISA_TABLE)
    missing_in_both_tables = find_missing_ids(gcs_ids, pdv_ids, pesquisa_ids)

    if FIX:
        logging.info("Moving folders to trigger the Cloud Function...")
        move_folders_for_triggering(TARGET_BUCKET, STAGING_BUCKET, [path for path in folder_paths if re.search(r'(\d+T\d+)-(\d+)-([-\w]+)', path).group(2) in missing_in_both_tables])

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    main()
