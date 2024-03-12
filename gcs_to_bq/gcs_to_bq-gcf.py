import json
import logging
import re
import os
from datetime import datetime

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from google.cloud.pubsub_v1.publisher.exceptions import RetryError, TimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential

DATASET_ID = os.getenv('DATASET_ID')
FILENAME_PATTERN = os.getenv('FILENAME_PATTERN')
SOURCE = os.getenv('SOURCE')
VERSION = os.getenv('VERSION')
PROJECT_ID = os.getenv('PROJECT_ID')
TOPIC_ID = os.getenv('TOPIC_ID')

logging.basicConfig(level=logging.DEBUG)

PDV_SCHEMA =[
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
	bigquery.SchemaField("parcelas", "RECORD", mode="REPEATED", fields=[
	bigquery.SchemaField("formaPagamento", "STRING"),
	bigquery.SchemaField("dataVencimento", "DATE"),
	bigquery.SchemaField("valor", "FLOAT"),
	bigquery.SchemaField("tPag", "STRING"),
	]),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("update_timestamp", "TIMESTAMP"),
]


PESQUISA_SCHEMA = [
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


PRODUTO_SCHEMA SCHEMA = [
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


def parse_filename(filename: str) -> Tuple[str, str, str]:
    """
    Parse the filename to extract the UUID, timestamp, and product type.

    Args:
        filename (str): The name of the file to parse.

    Returns:
        Tuple[str, str, str]: A tuple containing the UUID, timestamp, and product type.

    Raises:
        ValueError: If the filename does not match the expected pattern or contains an unexpected number of groups.
    """
    match = re.search(FILENAME_PATTERN, filename)
    if not match:
        logging.error(f"Error parsing filename {filename}: Filename does not match expected pattern.")
        raise ValueError("Filename does not match expected pattern.")

    groups = match.groups()
    if len(groups) == 5:
        _, product_type, _, timestamp_str, uuid = groups
    elif len(groups) == 4:
        _, product_type, timestamp_str, uuid = groups
    else:
        logging.error(f"Unexpected number of groups found in filename {filename}")
        raise ValueError("Unexpected number of groups in filename.")

    timestamp = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%S").isoformat()
    return uuid, timestamp, product_type


def ensure_table_exists(client: bigquery.Client, table_id: str, schema: List[bigquery.SchemaField]) -> None:
    """
    Ensure that the specified BigQuery table exists, creating it if necessary.

    Args:
        client (bigquery.Client): The BigQuery client.
        table_id (str): The ID of the table to check or create.
        schema (List[bigquery.SchemaField]): The schema of the table.
    """
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        logging.debug(f"Table {table_id} already exists.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="timestamp")
        client.create_table(table)
        logging.info(f"Table {table_id} created with day-partitioning on 'timestamp'.")


def transform_date_format(date_str: str) -> str:
    """
    Transform the date format from "dd/mm/yyyy" to "yyyy-mm-dd".

    Args:
        date_str (str): The date string to transform.

    Returns:
        str: The transformed date string, or the original string if the transformation fails.
    """
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError as e:
        logging.warning(f"Error transforming date format: {e}")
        return date_str


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=60))
def publish_to_pubsub(uuid: str) -> None:
    """
    Publish a message to a Pub/Sub topic with the client's pedido UUID, with retry logic for retryable errors.

    Args:
        uuid (str): The UUID of the pedido.

    Raises:
        TimeoutError: If a timeout occurs while publishing the message.
        RetryError: If the maximum number of retries is reached.
    """
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


def transform_and_load_pdv_data(client: bigquery.Client, storage_client: storage.Client, bucket_name: str, filename: str, uuid: str, timestamp: str) -> None:
    """
    Transform and load PDV data into BigQuery.

    Args:
        client (bigquery.Client): The BigQuery client.
        storage_client (storage.Client): The Google Cloud Storage client.
        bucket_name (str): The name of the bucket containing the file.
        filename (str): The name of the file to process.
        uuid (str): The UUID of the pedido.
        timestamp (str): The timestamp of the pedido.
    """
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
        'source_id': f"{SOURCE}-pdv_{VERSION}",
        'update_timestamp': datetime.utcnow().isoformat()
    })

    table_ref = client.dataset(DATASET_ID).table('pdv')
    errors = client.insert_rows_json(table_ref, [pedido_data])
    if errors:
        logging.error(f"Errors streaming data to BigQuery: {errors}")
    else:
        logging.info(f"Data streamed successfully to pdv.")
        publish_to_pubsub(uuid)


def transform_and_load_pesquisa_data(client: bigquery.Client, storage_client: storage.Client, bucket_name: str, filename: str, uuid: str, timestamp: str) -> None:
    """
    Transform and load Pesquisa data into BigQuery.

    Args:
        client (bigquery.Client): The BigQuery client.
        storage_client (storage.Client): The Google Cloud Storage client.
        bucket_name (str): The name of the bucket containing the file.
        filename (str): The name of the file to process.
        uuid (str): The UUID of the pedido.
        timestamp (str): The timestamp of the pedido.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    json_content = json.loads(blob.download_as_text())
    pedidos_data = json_content['retorno']['pedidos']

    for pedido in pedidos_data:
        pedido_data = pedido.get('pedido', {})
        if not pedido_data:
            logging.debug(f"Skipping empty pedido data: {pedido}")
            continue

        pedido_data['data_pedido'] = transform_date_format(pedido_data.get('data_pedido', ''))
        pedido_data['data_prevista'] = transform_date_format(pedido_data.get('data_prevista', ''))
        pedido_data.update({
            'uuid': uuid,
            'timestamp': datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").isoformat(),
            'source_id': f"{SOURCE}-pesquisa_{VERSION}",
            'update_timestamp': datetime.utcnow().isoformat()
        })

        table_ref = client.dataset(DATASET_ID).table('pesquisa')
        errors = client.insert_rows_json(table_ref, [pedido_data])
        if errors:
            logging.error(f"Errors streaming data to BigQuery: {errors}")
        else:
            logging.info(f"Data streamed successfully to pesquisa.")


def transform_and_load_produto_data(client: bigquery.Client, storage_client: storage.Client, bucket_name: str, filename: str, uuid: str, timestamp: str) -> None:
    """
    Transform and load Produto data into BigQuery.

    Args:
        client (bigquery.Client): The BigQuery client.
        storage_client (storage.Client): The Google Cloud Storage client.
        bucket_name (str): The name of the bucket containing the file.
        filename (str): The name of the file to process.
        uuid (str): The UUID of the produto.
        timestamp (str): The timestamp of the produto.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    json_content = json.loads(blob.download_as_text())
    produto_data = json_content['retorno']['produto']

    produto_data.update({
        'uuid': uuid,
        'timestamp': datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").isoformat(),
        'source_id': f"{SOURCE}-produto_{VERSION}",
        'update_timestamp': datetime.utcnow().isoformat()
    })

    table_ref = client.dataset(DATASET_ID).table('produto')
    errors = client.insert_rows_json(table_ref, [produto_data])
    if errors:
        logging.error(f"Errors streaming data to BigQuery: {errors}")
    else:
        logging.info(f"Data streamed successfully to produto.")


def cloud_function_entry_point(event: dict, context: Any) -> None:
    """
    The entry point for the Cloud Function.

    Args:
        event (dict): The event payload.
        context (Any): The event context.
    """
    client = bigquery.Client()
    storage_client = storage.Client()
    uuid, timestamp, product_type = parse_filename(event['name'])

    if product_type == 'pdv':
        ensure_table_exists(client, 'pdv', PDV_SCHEMA)
        transform_and_load_pdv_data(client, storage_client, event['bucket'], event['name'], uuid, timestamp)
    elif product_type == 'pesquisa':
        ensure_table_exists(client, 'pesquisa', PESQUISA_SCHEMA)
        transform_and_load_pesquisa_data(client, storage_client, event['bucket'], event['name'], uuid, timestamp)
    elif product_type == 'produto':
        ensure_table_exists(client, 'produto', PRODUTO_SCHEMA)
        transform_and_load_produto_data(client, storage_client, event['bucket'], event['name'], uuid, timestamp)
    else:
        logging.warning(f"Unsupported product type: {product_type}. Skipping.")
