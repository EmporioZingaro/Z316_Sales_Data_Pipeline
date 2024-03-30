import re
import os
import json
import base64
import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

from google.cloud import pubsub_v1
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from google.api_core import retry, exceptions
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

DATASET_ID = os.getenv('DATASET_ID')
SOURCE = os.getenv('SOURCE')
VERSION = os.getenv('VERSION')
PROJECT_ID = os.getenv('PROJECT_ID')
TOPIC_ID = os.getenv('TOPIC_ID')
NOTIFY = os.getenv('NOTIFY', 'False').lower() == 'true'

logging.basicConfig(level=logging.INFO)

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


PESQUISA_SCHEMA =[
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


PRODUTO_SCHEMA =[
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

DATA_TYPE_MAPPING = {
    'pedidos.pesquisa': {'table_name': 'pesquisa', 'schema': PESQUISA_SCHEMA},
    'pdv.pedido': {'table_name': 'pdv', 'schema': PDV_SCHEMA},
    'produto': {'table_name': 'produto', 'schema': PRODUTO_SCHEMA}
}


def ensure_table_exists(client: bigquery.Client, table_id: str, schema: List[bigquery.SchemaField]) -> None:
    logging.debug(f"Checking if table {table_id} exists")
    dataset_ref = client.dataset(DATASET_ID, project=PROJECT_ID)
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        logging.debug(f"Table {table_id} already exists.")
    except NotFound:
        logging.info(f"Table {table_id} does not exist. Creating table with day-partitioning on 'timestamp'.")
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="timestamp")
        client.create_table(table)
        logging.info(f"Table {table_id} created successfully.")


def log_bigquery_reference(client: bigquery.Client, dataset_id: str, table_id: str) -> None:
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"
    logging.info(f"BigQuery table reference: {full_table_id}")


def transform_date_format(date_str: str) -> str:
    logging.debug(f"Transforming date format for: {date_str}")
    try:
        transformed_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        logging.debug(f"Transformed date: {transformed_date}")
        return transformed_date
    except ValueError as e:
        logging.warning(f"Error transforming date format: {e}")
        return date_str


@retry(retry=retry_if_exception_type(exceptions.DeadlineExceeded), wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(3))
def publish_to_pubsub(uuid: str) -> None:
    if not NOTIFY:
        logging.info(f"Notification disabled. Skipping publishing message to {TOPIC_ID} with UUID: {uuid}")
        return
    try:
        logging.info(f"Publishing message to {TOPIC_ID} with UUID: {uuid}")
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
        message_data = json.dumps({"pedido_uuid": uuid}).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        future.result(timeout=30)
        logging.info(f"Published message to {TOPIC_ID} with UUID: {uuid}")
    except exceptions.DeadlineExceeded:
        logging.error(f"Timeout occurred while publishing message to {TOPIC_ID} with UUID: {uuid}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while publishing message to {TOPIC_ID} with UUID: {uuid}: {e}")


@retry(retry=retry_if_exception_type(exceptions.ServerError), wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(3))
def insert_rows_with_retry(client: bigquery.Client, table_ref: bigquery.TableReference, rows: List[Dict[str, Any]]) -> None:
    logging.info(f"Inserting {len(rows)} rows into {table_ref.table_id}")
    try:
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logging.error(f"Errors streaming data to BigQuery: {errors}")
        else:
            logging.info(f"Data streamed successfully to {table_ref.table_id}.")
    except exceptions.ServerError as e:
        logging.error(f"Server error occurred while inserting rows to {table_ref.table_id}: {e}")
        raise


def transform_and_load_pdv_data(client: bigquery.Client, pdv_data: dict, uuid: str, timestamp: str) -> None:
    logging.info("Transforming and loading PDV data.")
    ensure_table_exists(client, 'pdv', PDV_SCHEMA)

    pedido_data = pdv_data['retorno']['pedido']

    if 'data' in pedido_data:
        pedido_data['data'] = transform_date_format(pedido_data['data'])

    if 'parcelas' in pedido_data:
        for parcela in pedido_data['parcelas']:
            if 'dataVencimento' in parcela:
                parcela['dataVencimento'] = transform_date_format(parcela['dataVencimento'])

    pedido_data.update({
        'uuid': uuid,
        'timestamp': datetime.strptime(timestamp, "%Y%m%dT%H%M%S").isoformat(),
        'source_id': f"{SOURCE}-pdv_{VERSION}",
        'update_timestamp': datetime.utcnow().isoformat()
    })

    log_bigquery_reference(client, DATASET_ID, 'pdv')

    table_ref = client.dataset(DATASET_ID).table('pdv')
    insert_rows_with_retry(client, table_ref, [pedido_data])

    if NOTIFY:
        publish_to_pubsub(uuid)

    logging.info("PDV data transformation and loading completed.")


def transform_and_load_pesquisa_data(client: bigquery.Client, pesquisa_data: dict, uuid: str, timestamp: str) -> None:
    logging.info("Transforming and loading Pesquisa data.")
    ensure_table_exists(client, 'pesquisa', PESQUISA_SCHEMA)

    for pedido in pesquisa_data['retorno']['pedidos']:
        pedido_data = pedido['pedido']

        pedido_data['data_pedido'] = transform_date_format(pedido_data.get('data_pedido', ''))

        data_prevista = pedido_data.get('data_prevista', '')
        if data_prevista:
            pedido_data['data_prevista'] = transform_date_format(data_prevista)
        else:
            del pedido_data['data_prevista']

        pedido_data.update({
            'uuid': uuid,
            'timestamp': datetime.strptime(timestamp, "%Y%m%dT%H%M%S").isoformat(),
            'source_id': f"{SOURCE}-pesquisa_{VERSION}",
            'update_timestamp': datetime.utcnow().isoformat()
        })

        log_bigquery_reference(client, DATASET_ID, 'pesquisa')

        table_ref = client.dataset(DATASET_ID).table('pesquisa')

        insert_rows_with_retry(client, table_ref, [pedido_data])

        if NOTIFY:
            publish_to_pubsub(uuid)

    logging.info("Pesquisa data transformation and loading completed.")


def transform_and_load_produto_data(client: bigquery.Client, produto_data: dict, uuid: str, timestamp: str) -> None:
    logging.info("Transforming and loading Produto data.")
    ensure_table_exists(client, 'produto', PRODUTO_SCHEMA)

    if not produto_data:
        logging.debug("Received empty produto data.")
        return

    produto_data.update({
        'uuid': uuid,
        'timestamp': datetime.strptime(timestamp, "%Y%m%dT%H%M%S").isoformat(),
        'source_id': f"{SOURCE}-produto_{VERSION}",
        'update_timestamp': datetime.utcnow().isoformat()
    })

    log_bigquery_reference(client, DATASET_ID, 'produto')

    table_ref = client.dataset(DATASET_ID).table('produto')

    insert_rows_with_retry(client, table_ref, [produto_data])

    if NOTIFY:
        publish_to_pubsub(uuid)

    logging.info("Produto data transformation and loading completed.")


def cloud_function_entry_point(event: dict, context: Any) -> None:
    logging.info(f"Cloud Function triggered by Pub/Sub message: {event}")
    client = bigquery.Client()
    message_data = base64.b64decode(event['data']).decode('utf-8')
    message_json = json.loads(message_data)
    uuid = message_json.get("uuid")
    timestamp = message_json.get("timestamp")
    if not uuid or not timestamp:
        logging.error("UUID or Timestamp missing in Pub/Sub message.")
        return
    if "pdv_pedido_data" in message_json:
        pdv_pedido_data = message_json["pdv_pedido_data"]
        transform_and_load_pdv_data(client, pdv_pedido_data, uuid, timestamp)
    if "produto_data" in message_json:
        produto_data_list = message_json["produto_data"]
        for produto_data in produto_data_list:
            if "retorno" in produto_data and "produto" in produto_data["retorno"]:
                transform_and_load_produto_data(client, produto_data["retorno"]["produto"], uuid, timestamp)
    if "pedidos_pesquisa_data" in message_json:
        pedidos_pesquisa_data = message_json["pedidos_pesquisa_data"]
        transform_and_load_pesquisa_data(client, pedidos_pesquisa_data, uuid, timestamp)
    logging.info("Processing completed for Pub/Sub message.")
