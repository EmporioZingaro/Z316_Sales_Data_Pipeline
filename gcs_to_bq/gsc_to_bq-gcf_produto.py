import re
import json
import logging
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

#DATASET_ID = 'z316_tiny_raw_json'
DATASET_ID = 'z316_test'
TABLE_ID = 'produto'
FILENAME_PATTERN = r"z316-tiny-api-\d+-(produto|pdv|pesquisa)(-\d+)?-(\d{8}T\d{6})-([a-f0-9-]+)\.json"
SOURCE = 'cloudfunction-produto'
VERSION = 'v2'
SOURCE_ID = f"{SOURCE}_{VERSION}"


logging.basicConfig(level=logging.INFO)

SCHEMA = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("nome", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("codigo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unidade", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("preco", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("preco_promocional", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ncm", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("origem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gtin", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gtin_embalagem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("localizacao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("peso_liquido", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("peso_bruto", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("estoque_minimo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("estoque_maximo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("id_fornecedor", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("nome_fornecedor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("codigo_fornecedor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("codigo_pelo_fornecedor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unidade_por_caixa", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("preco_custo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("preco_custo_medio", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("situacao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("classe_ipi", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("valor_ipi_fixo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cod_lista_servicos", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("descricao_complementar", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("garantia", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cest", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("obs", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipoVariacao", "STRING"),
    bigquery.SchemaField("variacoes", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("idProdutoPai", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("sob_encomenda", "STRING"),
    bigquery.SchemaField("dias_preparacao", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("marca", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipoEmbalagem", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("alturaEmbalagem", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("larguraEmbalagem", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("comprimentoEmbalagem", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("diametroEmbalagem", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("qtd_volumes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("categoria", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("anexos", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("anexo", "STRING"),
    ]),
    bigquery.SchemaField("imagens_externas", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("url", "STRING"),
    ]),
    bigquery.SchemaField("classe_produto", "STRING"),
    bigquery.SchemaField("seo_title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("seo_keywords", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("link_video", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("seo_description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("slug", "STRING", mode="NULLABLE"),
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

    produto_data = json_content['retorno']['produto']

    # Add new fields and ensure datetime objects are formatted as strings
    produto_data.update({
        'uuid': uuid,
        'timestamp': datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").isoformat(),  # Ensure timestamp is ISO formatted
        'source_id': SOURCE_ID,
        'update_timestamp': datetime.utcnow().isoformat()  # Use ISO format for update_timestamp
    })

    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    errors = client.insert_rows_json(table_ref, [produto_data])
    if errors:
        logging.error(f"Errors streaming data to BigQuery: {errors}")
    else:
        logging.info(f"Data streamed successfully to {TABLE_ID}.")

def cloud_function_entry_point(event, context):
    if "produto" not in event['name']:
        logging.info(f"The file {event['name']} does not contain 'produto'. Skipping.")
        return

    client = bigquery.Client()
    storage_client = storage.Client()

    uuid, timestamp = parse_filename(event['name'])
    ensure_table_exists(client)
    transform_and_load_data(client, storage_client, event['bucket'], event['name'], uuid, timestamp)
