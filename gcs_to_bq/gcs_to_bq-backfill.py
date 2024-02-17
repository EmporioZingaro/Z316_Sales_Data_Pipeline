import re
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential

PROJECT_ID = 'emporio-zingaro'
DATASET_ID = 'z316_test'
BUCKET_NAME = 'z316-test-bucket'
FILENAME_PATTERN = r".*/z316-tiny-api-(\d+)-(produto|pdv|pesquisa)(?:-(\d+))?-(\d{8}T\d{6})-([a-f0-9-]+)\.json"
SOURCE = 'buckettobq-backfill'
VERSION = 'vFinal'
SOURCE_ID = f"{SOURCE}_{VERSION}"

bigquery_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_date_format(date_str):
    if date_str:
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError as e:
            logging.error(f"Error transforming date format for date {date_str}: {e}")
    return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def parse_filename(filename):
    match = re.search(FILENAME_PATTERN, filename)
    if match:
        pedido_id, jsontype, produto_id, timestamp_str, uuid = match.groups()
        timestamp = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%S").isoformat()
        logging.info(f"Filename parsed successfully: {filename}")
        return jsontype, uuid, timestamp, produto_id
    else:
        logging.error(f"Error parsing filename {filename}: Filename does not match expected pattern.")
        return None, None, None, None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def check_bigquery_for_existing_record(client, uuid, jsontype, produto_id=None):
    logging.info(f"Checking for existing record in BigQuery: UUID={uuid}, Type={jsontype}, Produto ID={produto_id}")
    query = f"SELECT uuid FROM `{PROJECT_ID}.{DATASET_ID}.{jsontype}` WHERE uuid = @uuid"
    parameters = [bigquery.ScalarQueryParameter("uuid", "STRING", uuid)]

    if jsontype == 'produto' and produto_id:
        query += " AND id = @produto_id"
        parameters.append(bigquery.ScalarQueryParameter("produto_id", "INT64", int(produto_id)))

    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    query_job = client.query(query, job_config=job_config)
    results = list(query_job.result())
    exists = len(results) > 0
    logging.info(f"Record exists: {exists}")
    return exists

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def transform_and_load_data(client, storage_client, filename, jsontype, uuid, timestamp, produto_id=None):
    logging.info(f"Processing blob: {filename} with type: {jsontype}")
    blob = storage_client.bucket(BUCKET_NAME).blob(filename)
    json_content = json.loads(blob.download_as_text())

    if jsontype == 'pdv':
        data = transform_pdv_data(json_content, uuid, timestamp)
    elif jsontype == 'pesquisa':
        data = transform_pesquisa_data(json_content, uuid, timestamp)
    elif jsontype == 'produto':
        data = transform_produto_data(json_content, uuid, timestamp, produto_id)
    else:
        logging.error(f"Unsupported JSON type: {jsontype}")
        return

    load_data_to_bigquery(client, data, jsontype)

def transform_pdv_data(pdv_data, uuid, timestamp):
    pedido_data = pdv_data['retorno']['pedido']
    if 'data' in pedido_data:
        pedido_data['data'] = transform_date_format(pedido_data['data'])
    if 'parcelas' in pedido_data:
        for parcela in pedido_data['parcelas']:
            if 'dataVencimento' in parcela:
                parcela['dataVencimento'] = transform_date_format(parcela['dataVencimento'])

    pedido_data.update({
        'uuid': uuid,
        'timestamp': timestamp,
        'source_id': SOURCE_ID,
        'update_timestamp': datetime.utcnow().isoformat()
    })
    return pedido_data

def transform_pesquisa_data(pesquisa_data, uuid, timestamp):
    transformed_data = []
    pedidos_data = pesquisa_data['retorno']['pedidos']
    for pedido in pedidos_data:
        pedido_data = pedido.get('pedido', {})
        if pedido_data:
            pedido_data['data_pedido'] = transform_date_format(pedido_data.get('data_pedido', ''))
            pedido_data['data_prevista'] = transform_date_format(pedido_data.get('data_prevista', ''))

            pedido_data.update({
                'uuid': uuid,
                'timestamp': timestamp,
                'source_id': SOURCE_ID,
                'update_timestamp': datetime.utcnow().isoformat()
            })
            transformed_data.append(pedido_data)
    return transformed_data

def transform_produto_data(produto_data, uuid, timestamp, produto_id):
    produto_info = produto_data['retorno']['produto']
    produto_info.update({
        'uuid': uuid,
        'timestamp': timestamp,
        'source_id': SOURCE_ID,
        'update_timestamp': datetime.utcnow().isoformat(),
        'id': produto_id
    })
    return produto_info

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def load_data_to_bigquery(client, data, jsontype):
    logging.info(f"Loading data to BigQuery table: {jsontype}")
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{jsontype}"
    errors = client.insert_rows_json(table_id, [data] if isinstance(data, dict) else data)
    if errors:
        logging.error(f"Errors streaming data to BigQuery for {jsontype}: {errors}")
    else:
        logging.info(f"Data streamed successfully to BigQuery table {jsontype}.")

def main():
    logging.info("Starting the backfill process.")
    blobs = storage_client.bucket(BUCKET_NAME).list_blobs()
    for blob in blobs:
        jsontype, uuid, timestamp, produto_id = parse_filename(blob.name)
        if jsontype:
            if not check_bigquery_for_existing_record(bigquery_client, uuid, jsontype, produto_id):
                transform_and_load_data(bigquery_client, storage_client, blob.name, jsontype, uuid, timestamp, produto_id)
        else:
            logging.info(f"Skipping blob due to parsing issues or unsupported type: {blob.name}")

if __name__ == "__main__":
    main()
