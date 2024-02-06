import re
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

#DATASET_ID = 'z316_tiny_raw_json'
DATASET_ID = 'z316_test'
TABLE_ID = 'pesquisa'
FILENAME_PATTERN = r"z316-tiny-api-\d+-(produto|pdv|pesquisa)(-\d+)?-(\d{8}T\d{6})-([a-f0-9-]+)\.json"
SOURCE = 'cloudfunction-pesquisa'
VERSION = 'v2'
SOURCE_ID = f"{SOURCE}_{VERSION}"

logging.basicConfig(level=logging.INFO)

SCHEMA = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("numero", "STRING"),
    bigquery.SchemaField("numero_ecommerce", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_pedido", "DATE"),
    bigquery.SchemaField("data_prevista", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("nome", "STRING"),
    bigquery.SchemaField("valor", "FLOAT"),
    bigquery.SchemaField("id_vendedor", "STRING"),
    bigquery.SchemaField("nome_vendedor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("situacao", "STRING"),
    bigquery.SchemaField("codigo_rastreamento", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("url_rastreamento", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("update_timestamp", "TIMESTAMP"),
]

def parse_filename(filename):
    match = re.search(FILENAME_PATTERN, filename)
    if match:
        groups = match.groups()
        if len(groups) == 5:
            _, product_type, _, timestamp_str, uuid = groups
        elif len(groups) == 4:
            _, product_type, timestamp_str, uuid = groups
        else:
            logging.error(f"Unexpected number of groups found in filename {filename}")
            raise ValueError("Unexpected number of groups in filename.")
        
        timestamp = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%S").isoformat()
        return uuid, timestamp
    else:
        logging.error(f"Error parsing filename {filename}: Filename does not match expected pattern.")
        raise ValueError("Filename does not match expected pattern.")

def ensure_table_exists(client):
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)
    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(field="timestamp")
        client.create_table(table)
        logging.info(f"Table {TABLE_ID} created with day-partitioning on 'timestamp'.")

def transform_and_load_data(client, storage_client, bucket_name, filename, uuid, timestamp):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    json_content = json.loads(blob.download_as_text())

    pedidos_data = json_content['retorno']['pedidos']
    for pedido in pedidos_data:
        pedido_data = pedido.get('pedido', {})
        if not pedido_data:
            continue

        pedido_data['data_pedido'] = transform_date_format(pedido_data.get('data_pedido', ''))
        pedido_data['data_prevista'] = transform_date_format(pedido_data.get('data_prevista', ''))

        pedido_data.update({
            'uuid': uuid,
            'timestamp': datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").isoformat(),
            'source_id': SOURCE_ID,
            'update_timestamp': datetime.utcnow().isoformat()
        })

        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        errors = client.insert_rows_json(table_ref, [pedido_data])
        if errors:
            logging.error(f"Errors streaming data to BigQuery: {errors}")
        else:
            logging.info(f"Data streamed successfully to {TABLE_ID}.")

def transform_date_format(date_str):
    if date_str:
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError as e:
            logging.error(f"Error transforming date format for date {date_str}: {e}")
    return None

def cloud_function_entry_point(event, context):
    if "pesquisa" not in event['name']:
        logging.info(f"The file {event['name']} does not contain 'pesquisa'. Skipping.")
        return

    client = bigquery.Client()
    storage_client = storage.Client()

    uuid, timestamp = parse_filename(event['name'])
    ensure_table_exists(client)
    transform_and_load_data(client, storage_client, event['bucket'], event['name'], uuid, timestamp)
