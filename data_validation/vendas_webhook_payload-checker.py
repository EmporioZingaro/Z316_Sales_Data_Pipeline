import json
import logging
import google.cloud.storage as storage

logging.basicConfig(level=logging.INFO)

BUCKET_NAME = "z316-tiny-webhook"
DRY_RUN = False

def validate_payload(data):
    required_fields = ["versao", "cnpj", "tipo", "dados"]
    if not all(field in data for field in required_fields):
        logging.info("Payload missing required fields")
        return False
    if data["tipo"] != "inclusao_pedido":
        logging.info(f"Payload 'tipo' is not 'inclusao_pedido': {data['tipo']}")
        return False
    return True

def check_file_for_deletion(blob):
    data = json.loads(blob.download_as_string())
    is_valid = validate_payload(data)

    if not is_valid:
        logging.info(f"File {blob.name} does not meet validation criteria and will be marked for deletion.")
        return True
    else:
        logging.info(f"File {blob.name} meets all validation criteria. Skipping deletion.")
        return False

def delete_marked_files(storage_client, files_to_delete):
    for blob in files_to_delete:
        if DRY_RUN:
            logging.info(f"DRY RUN: Would delete {blob.name}")
        else:
            logging.info(f"Deleting {blob.name}")
            blob.delete()

def main():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)


    blobs = list(bucket.list_blobs(prefix="vendas/"))
    files_to_delete = []

    for blob in blobs:
        try:
            if check_file_for_deletion(blob):
                files_to_delete.append(blob)
        except Exception as e:
            logging.error(f"Error processing file {blob.name}: {e}")

    delete_marked_files(storage_client, files_to_delete)

    if DRY_RUN:
        logging.info("DRY RUN Complete: No files were deleted.")
    else:
        logging.info("Deletion process complete.")

if __name__ == "__main__":
    main()
