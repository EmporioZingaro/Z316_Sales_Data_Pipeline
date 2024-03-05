import re
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.pubsub_v1.publisher.exceptions import TimeoutError, RetryError
from tenacity import retry, stop_after_attempt, wait_exponential

DATASET_ID = 'z316_tiny_raw_json'
TABLE_ID = 'pdv'
FILENAME_PATTERN = r"z316-tiny-api-\d+-(produto|pdv|pesquisa)(-\d+)?-(\d{8}T\d{6})-([a-f0-9-]+)\.json"
SOURCE = 'cloudfunction-pdv'
VERSION = 'v2'
SOURCE_ID = f"{SOURCE}_{VERSION}"

logging.basicConfig(level=logging.INFO)

SCHEMA =[
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
	bigquery.SchemaField("id", "INTEGER"),
	bigquery.SchemaField("numero", "INTEGER"),
	bigquery.SchemaField("data", "DATE"),
	bigquery.SchemaField("frete", "FLOAT"),
	bigquery.SchemaField("desconto", "STRING"),
	bigquery.SchemaField("valorICMSSubst", "FLOAT"),
	bigquery.SchemaField("valorIPI", "FLOAT"),
	bigquery.SchemaField("totalProdutos", "FLOAT"),
	bigquery.SchemaField("totalVenda", "FLOAT"),
	bigquery.SchemaField("fretePorConta", "STRING"),
	bigquery.SchemaField("pesoLiquido", "FLOAT"),
	bigquery.SchemaField("pesoBruto", "FLOAT"),
	bigquery.SchemaField("observacoes", "STRING"),
	bigquery.SchemaField("formaPagamento", "STRING"),
	bigquery.SchemaField("situacao", "STRING"),
	# Nested 'contato' record
	bigquery.SchemaField("contato", "RECORD", fields=[
	bigquery.SchemaField("nome", "STRING"),
	bigquery.SchemaField("fantasia", "STRING"),
	bigquery.SchemaField("codigo", "STRING"),
	bigquery.SchemaField("tipo", "STRING"),
	bigquery.SchemaField("cpfCnpj", "STRING"),
	bigquery.SchemaField("endereco", "STRING"),
	bigquery.SchemaField("enderecoNro", "STRING"),
	bigquery.SchemaField("complemento", "STRING"),
	bigquery.SchemaField("bairro", "STRING"),
	bigquery.SchemaField("cidade", "STRING"),
	bigquery.SchemaField("uf", "STRING"),
	bigquery.SchemaField("cep", "STRING"),
	bigquery.SchemaField("fone", "STRING"),
	bigquery.SchemaField("celular", "STRING"),
	bigquery.SchemaField("email", "STRING"),
	bigquery.SchemaField("inscricaoEstadual", "STRING"),
	bigquery.SchemaField("indIEDest", "STRING"),
	]),
	# Nested 'enderecoEntrega' record
	bigquery.SchemaField("enderecoEntrega", "RECORD", fields=[
	bigquery.SchemaField("nome", "STRING"),
	bigquery.SchemaField("tipo", "STRING"),
	bigquery.SchemaField("cpfCnpj", "STRING"),
	bigquery.SchemaField("endereco", "STRING"),
	bigquery.SchemaField("enderecoNro", "STRING"),
	bigquery.SchemaField("complemento", "STRING"),
	bigquery.SchemaField("bairro", "STRING"),
	bigquery.SchemaField("cidade", "STRING"),
	bigquery.SchemaField("uf", "STRING"),
	bigquery.SchemaField("cep", "STRING"),
	bigquery.SchemaField("fone", "STRING"),
	]),
	# Repeated 'itens' record
	bigquery.SchemaField("itens", "RECORD", mode="REPEATED", fields=[
	bigquery.SchemaField("id", "INTEGER"),
	bigquery.SchemaField("idProduto", "INTEGER"),
	bigquery.SchemaField("descricao", "STRING"),
	bigquery.SchemaField("codigo", "STRING"),
	bigquery.SchemaField("valor", "FLOAT"),
	bigquery.SchemaField("quantidade", "FLOAT"),
	bigquery.SchemaField("desconto", "STRING"),
	bigquery.SchemaField("pesoLiquido", "FLOAT"),
	bigquery.SchemaField("pesoBruto", "FLOAT"),
	bigquery.SchemaField("unidade", "STRING"),
	bigquery.SchemaField("tipo", "STRING"),
	bigquery.SchemaField("ncm", "STRING"),
	bigquery.SchemaField("origem", "STRING"),
	bigquery.SchemaField("cest", "STRING"),
	bigquery.SchemaField("gtin", "STRING"),
	bigquery.SchemaField("gtinTributavel", "STRING"),
	]),
	# Repeated 'parcelas' record
	bigquery.SchemaField("parcelas", "RECORD", mode="REPEATED", fields=[
	bigquery.SchemaField("formaPagamento", "STRING"),
	bigquery.SchemaField("dataVencimento", "DATE"),
	bigquery.SchemaField("valor", "FLOAT"),
	bigquery.SchemaField("tPag", "STRING"),
	]),
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

def transform_date_format(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        logging.error(f"Error transforming date format: {e}")
        return date_str

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=60))
def publish_to_pubsub(uuid):
    """Publishes a message to a Pub/Sub topic with the client's pedido UUID, with retry logic for retryable errors."""
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
        message_data = json.dumps({"pedido_uuid": uuid}).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        future.result(timeout=30)
        logging.info(f"Published message to {TOPIC_ID} with UUID: {uuid}")
    except TimeoutError:
        logging.error(f"Timeout occurred while publishing message to {TOPIC_ID} with UUID: {uuid}")
        raise
    except RetryError as e:
        logging.error(f"Max retries reached while publishing message to {TOPIC_ID} with UUID: {uuid}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while publishing message to {TOPIC_ID} with UUID: {uuid}: {e}")

def transform_and_load_data(client, storage_client, bucket_name, filename, uuid, timestamp):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    json_content = json.loads(blob.download_as_text())

    pedido_data = json_content['retorno']['pedido']
    if 'data' in pedido_data:
        pedido_data['data'] = transform_date_format(pedido_data['data'])
    if 'parcelas' in pedido_data:
        for parcela in pedido_data['parcelas']:
            if 'dataVencimento' in parcela:
                parcela['dataVencimento'] = transform_date_format(parcela['dataVencimento'])

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
        publish_to_pubsub(uuid)

def cloud_function_entry_point(event, context):
    if "pdv" not in event['name']:
        logging.info(f"The file {event['name']} does not contain 'pdv'. Skipping.")
        return

    client = bigquery.Client()
    storage_client = storage.Client()

    uuid, timestamp = parse_filename(event['name'])
    ensure_table_exists(client)
    transform_and_load_data(client, storage_client, event['bucket'], event['name'], uuid, timestamp)
