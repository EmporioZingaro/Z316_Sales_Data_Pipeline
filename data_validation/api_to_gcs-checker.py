import json
import re
import logging
from google.cloud import storage, bigquery
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

BUCKET_NAME = "z316-tiny-api"
DRY_RUN = False
BIGQUERY_DATASET = "emporio-zingaro.z316_tiny_raw_json"

storage_client = storage.Client()
bigquery_client = bigquery.Client()

summary_entries = []
processed_folders = set()

all_blobs = list(storage_client.list_blobs(BUCKET_NAME))

def list_folders():
    folders = {blob.name.split('/')[0] for blob in all_blobs}
    logger.info(f"Found {len(folders)} folders to process.")
    return folders

def find_files(folder_name, file_type):
    target_file = next((blob for blob in all_blobs if file_type in blob.name and blob.name.startswith(folder_name)), None)
    log_action = "Found" if target_file else "No"
    log_level = logger.info if target_file else logger.warning
    log_level(f"[{'DRY RUN - ' if DRY_RUN else ''}{log_action}] {file_type} file in folder: {folder_name}")
    return target_file

def extract_ids(blob, id_type):
    content = blob.download_as_text()
    data = json.loads(content)
    ids = set()
    if id_type == 'produto':
        ids = {item.get('idProduto') for item in data.get('retorno', {}).get('pedido', {}).get('itens', [])}
    elif id_type == 'pedido':
        if 'pdv' in blob.name:
            ids.add(data.get('retorno', {}).get('pedido', {}).get('id'))
        elif 'pesquisa' in blob.name:
            pedidos = data.get('retorno', {}).get('pedidos', [])
            if pedidos:
                ids.add(pedidos[0].get('pedido', {}).get('id'))
    logger.info(f"[{'DRY RUN - ' if DRY_RUN else ''}Extracted] {len(ids)} {id_type} IDs from file: {blob.name}")
    return ids

def verify_produto_files(folder_name, produto_ids):
    missing_produto_files = False
    for produto_id in produto_ids:
        produto_file = find_files(folder_name, f"produto-{produto_id}")
        if not produto_file:
            missing_produto_files = True
            logger.warning(f"[{'DRY RUN - ' if DRY_RUN else ''}Missing] Produto file for ID {produto_id} in folder: {folder_name}")
    return not missing_produto_files

def check_files(folder_name):
    if folder_name in processed_folders:
        return True

    pdv_file = find_files(folder_name, 'pdv')
    pesquisa_file = find_files(folder_name, 'pesquisa')
    all_files_present = pdv_file and pesquisa_file

    if not all_files_present:
        uuid = extract_uuid(folder_name)
        summary_entries.append({'Folder': folder_name, 'UUID': uuid, 'Action': "Delete", 'Reason': "Missing Files"})
        processed_folders.add(folder_name)
        return False

    pdv_ids = extract_ids(pdv_file, 'pedido')
    pesquisa_ids = extract_ids(pesquisa_file, 'pedido')
    if pdv_ids != pesquisa_ids:
        uuid = extract_uuid(folder_name)
        summary_entries.append({'Folder': folder_name, 'UUID': uuid, 'Action': "Delete", 'Reason': "Mismatched IDs"})
        processed_folders.add(folder_name)
        return False

    produto_ids = extract_ids(pdv_file, 'produto')
    if not verify_produto_files(folder_name, produto_ids):
        uuid = extract_uuid(folder_name)
        summary_entries.append({'Folder': folder_name, 'UUID': uuid, 'Action': "Delete", 'Reason': "Missing Produto Files"})
        processed_folders.add(folder_name)
        return False

    processed_folders.add(folder_name)
    return True

def extract_uuid(folder_name):
    match = re.search(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", folder_name)
    return match.group(1) if match else None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def delete_blob(blob):
    if not DRY_RUN:
        blob.delete()

def delete_folder(folder_name):
    blobs = [blob for blob in all_blobs if blob.name.startswith(folder_name)]
    for blob in blobs:
        logger.warning(f"[{'DRY RUN - ' if DRY_RUN else ''}Deleting file] {blob.name}")
        delete_blob(blob)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def execute_bigquery(query):
    if DRY_RUN:
        logger.warning(f"[DRY RUN] Would execute query: {query}")
        return iter([[0]])
    else:
        return bigquery_client.query(query).result()

def delete_bigquery_records(uuid, folder_name):
    tables = ["pdv", "pesquisa", "produto"]
    for table in tables:
        deletion_query = f"DELETE FROM `{BIGQUERY_DATASET}.{table}` WHERE uuid = '{uuid}'"
        execute_bigquery(deletion_query)
        logger.warning(f"[{'DRY RUN - ' if DRY_RUN else ''}Deleting BigQuery records] from {table} with UUID: {uuid}")

        verification_query = f"SELECT COUNT(*) FROM `{BIGQUERY_DATASET}.{table}` WHERE uuid = '{uuid}'"
        result = execute_bigquery(verification_query)
        count = next(iter(result), [0])[0]

        if count == 0:
            logger.info(f"[{'DRY RUN - ' if DRY_RUN else ''}Verified deletion] from {table} with UUID: {uuid}")
        else:
            logger.error(f"[{'DRY RUN - ' if DRY_RUN else ''}Verification failed] Records from {table} with UUID: {uuid} were not fully deleted. Remaining count: {count}")

def group_folders_by_dados_id(folders):
    grouped_folders = {}
    for folder in folders:
        dados_id = extract_dados_id(folder)
        grouped_folders.setdefault(dados_id, []).append(folder)
    return grouped_folders

def extract_dados_id(folder_name):
    match = re.search(r"\d+T\d+-(\d+)-", folder_name)
    return match.group(1) if match else None

def handle_duplicate_folders(folders):
    folders_with_zero_time = [folder for folder in folders if 'T000000' in folder]
    folders_with_timestamps = [folder for folder in folders if folder not in folders_with_zero_time]

    if len(folders_with_zero_time) == len(folders):
        folder_to_keep = folders_with_zero_time[0]
    elif folders_with_zero_time and folders_with_timestamps:
        folder_to_keep = min(folders_with_timestamps, key=lambda x: x.split('-')[0])
    else:
        folder_to_keep = min(folders_with_timestamps, key=lambda x: x.split('-')[0])

    for folder in folders:
        if folder == folder_to_keep:
            logger.info(f"Keeping folder: {folder}")
        else:
            uuid = extract_uuid(folder)
            delete_folder(folder)
            delete_bigquery_records(uuid, folder)
            summary_entries.append({
                'Folder': folder,
                'UUID': uuid,
                'Action': "Delete",
                'Reason': f"{'T000000 present' if folders_with_zero_time else 'Older timestamp'} - Duplicate"
            })
            processed_folders.add(folder)

    logger.info(f"Folder kept after duplicate resolution: {folder_to_keep}")

def find_and_handle_duplicates(folders):
    grouped_folders = group_folders_by_dados_id(folders)
    for dados_id, duplicate_folders in grouped_folders.items():
        if len(duplicate_folders) > 1:
            handle_duplicate_folders(duplicate_folders)

def delete_folder_and_records(folder_name):
    uuid = extract_uuid(folder_name)
    if uuid:
        delete_folder(folder_name)
        delete_bigquery_records(uuid, folder_name)
        logger.warning(f"[{'DRY RUN - ' if DRY_RUN else ''}Deleted folder and records] {folder_name} with UUID {uuid}")
    else:
        logger.error(f"Failed to extract UUID from folder name: {folder_name}")

def print_summary():
    if summary_entries:
        print("\nSummary of Actions Taken:")
        for entry in summary_entries:
            print(f"Folder: {entry['Folder']}, UUID: {entry['UUID']}, Action: {entry['Action']}, Reason: {entry['Reason']}")
    else:
        print("No actions were recorded.")

def main():
    logger.warning("Starting the script...")
    folders = list_folders()
    total_folders = len(folders)
    processed_count = 0

    for folder in folders:
        if not check_files(folder):
            delete_folder_and_records(folder)
        processed_count += 1
        if processed_count % 1000 == 0 or processed_count == total_folders:
            logger.warning(f"[{'DRY RUN - ' if DRY_RUN else ''}Progress] Processed {processed_count}/{total_folders} folders.")

    find_and_handle_duplicates(folders)
    logger.warning("Script completed.")
    print_summary()

if __name__ == "__main__":
    main()
