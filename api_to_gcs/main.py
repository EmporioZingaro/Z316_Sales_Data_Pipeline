import json
import requests
import hashlib
import os
import logging
from datetime import datetime
from typing import Optional, Any, Tuple

from google.cloud import storage, secretmanager
from google.cloud import pubsub_v1
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

BASE_URL = os.environ['BASE_URL']
FILE_PREFIX = os.environ['FILE_PREFIX']
TARGET_BUCKET_NAME = os.environ['TARGET_BUCKET_NAME']
FOLDER_NAME = os.environ['FOLDER_NAME']
PDV_FILENAME = os.environ['PDV_FILENAME']
PESQUISA_FILENAME = os.environ['PESQUISA_FILENAME']
PRODUTO_FILENAME = os.environ['PRODUTO_FILENAME']
NFCe_FILENAME = os.environ['NFCE_FILENAME']
SECRET_PATH = os.environ['SECRET_PATH']
PROJECT_ID = os.environ['PROJECT_ID']
SOURCE_IDENTIFIER = os.environ['SOURCE_IDENTIFIER']
VERSION_CONTROL = os.environ['VERSION_CONTROL']
PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
secret_manager_client = secretmanager.SecretManagerServiceClient()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ValidationError(Exception):
    pass


class InvalidTokenError(Exception):
    pass


class RetryableError(Exception):
    pass


def get_api_token() -> str:
    try:
        logger.debug("Accessing API token from Secret Manager")
        response = secret_manager_client.access_secret_version(request={"name": SECRET_PATH})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.exception(f"Failed to access secret: {e}")
        raise


@retry(wait=wait_exponential(multiplier=2.5, min=30, max=187.5), stop=stop_after_attempt(4), retry=retry_if_exception_type((requests.exceptions.RequestException, RetryableError)))
def make_api_call(url: str) -> dict:
    try:
        sanitized_url = url.split('?token=')[0]
        logger.debug(f"Making API call to: {sanitized_url}")
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

        validate_json_payload(json_data)

        return json_data
    except requests.exceptions.RequestException as e:
        logger.exception(f"API request failed: {e}")
        raise
    except ValidationError as e:
        logger.exception(f"Payload validation failed: {e}")
        raise


def validate_json_payload(json_data: dict) -> None:
    status_processamento = json_data.get('retorno', {}).get('status_processamento')

    if status_processamento == '3':
        return
    elif status_processamento == '2':
        raise ValidationError("Invalid query parameter.")
    elif status_processamento == '1':
        codigo_erro = json_data.get('retorno', {}).get('codigo_erro')
        erros = json_data.get('retorno', {}).get('erros', [])
        erro_message = erros[0]['erro'] if erros else "Unknown error"
        if codigo_erro == '1':
            raise InvalidTokenError("Token is not valid: " + erro_message)
        else:
            raise RetryableError("Error encountered, will attempt retry: " + erro_message)


def read_webhook_payload(bucket_name: str, file_name: str) -> dict:
    try:
        logger.debug(f"Reading webhook payload from bucket: {bucket_name}, file: {file_name}")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        return json.loads(blob.download_as_string(client=None))
    except Exception as e:
        logger.exception(f"Failed to read webhook payload: {e}")
        raise


def process_webhook_payload(event: dict, context: Any) -> None:
    logger.info(f"Function execution started - Context: {context.event_id}")
    try:
        payload_details = extract_payload_details(event)
        if not payload_details:
            return

        dados_id, timestamp, uuid_str = payload_details
        token = get_api_token()

        pdv_pedido_data, pedido_numero, produto_data = process_pdv_pedido_data(*payload_details, token)
        pedidos_pesquisa_data = process_pedidos_pesquisa_data(*payload_details, token, pedido_numero)

        nota_fiscal_link_data = None
        try:
            nfce_id = process_nfce_generation(dados_id, token)
            nota_fiscal_link_data = process_nota_fiscal_link_retrieval(nfce_id, token, dados_id, timestamp, uuid_str, pedido_numero)
        except Exception as e:
            logger.exception(f"An error occurred during NFC-e generation or link fetching: {e}")

        publish_notification(PUBSUB_TOPIC, pdv_pedido_data, produto_data, pedidos_pesquisa_data, nota_fiscal_link_data, timestamp, uuid_str)

    except Exception as e:
        logger.exception(f"Function failed: {e} - Context: {context.event_id}")

    logger.info(f"Function execution completed successfully - Context: {context.event_id}")


def extract_payload_details(event: dict) -> Optional[Tuple[str, str, str]]:
    file_name = event['name']
    webhook_payload = read_webhook_payload(event['bucket'], file_name)
    dados_id = webhook_payload.get('dados', {}).get('id')

    if not dados_id:
        logger.warning("dados.id not found in webhook payload")
        return None

    parts = file_name.rstrip('.json').split('-')
    return dados_id, parts[-6], '-'.join(parts[-5:])


def process_pdv_pedido_data(dados_id: str, timestamp: str, uuid_str: str, token: str) -> Tuple[dict, str, list]:
    logger.debug(f"Processing PDV pedido data - dados_id: {dados_id}, timestamp: {timestamp}, uuid_str: {uuid_str}")
    folder_path = FOLDER_NAME.format(timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str)
    pdv_pedido_data = fetch_pdv_pedido_data(dados_id, token)
    pedido_numero = pdv_pedido_data.get('retorno', {}).get('pedido', {}).get('numero')
    store_payload(pdv_pedido_data, PDV_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
        'uuid_str': uuid_str,
        'pedido_id': pedido_numero,
        'data_type': 'pdv.pedido'
    })
    produto_payloads = []
    for item in pdv_pedido_data.get('retorno', {}).get('pedido', {}).get('itens', []):
        item_id = item.get('idProduto')
        if item_id:
            produto_payload = fetch_produto_data(item_id, token)
            produto_payloads.append(produto_payload)
            store_payload(produto_payload, PRODUTO_FILENAME.format(dados_id=dados_id, produto_id=item_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
                'uuid_str': uuid_str,
                'pedido_id': pedido_numero,
                'produto_id': item_id,
                'data_type': 'produto'
            })
    return pdv_pedido_data, pedido_numero, produto_payloads


def process_pedidos_pesquisa_data(dados_id: str, timestamp: str, uuid_str: str, token: str, pedido_numero: str) -> dict:
    logger.debug(f"Processing pedidos pesquisa data - dados_id: {dados_id}, timestamp: {timestamp}, uuid_str: {uuid_str}, pedido_numero: {pedido_numero}")
    folder_path = FOLDER_NAME.format(timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str)
    pedidos_data = fetch_pedidos_pesquisa_data(pedido_numero, token)
    store_payload(pedidos_data, PESQUISA_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
        'uuid_str': uuid_str,
        'pedido_id': pedido_numero,
        'data_type': 'pedidos.pesquisa'
    })
    return pedidos_data


def process_nfce_generation(dados_id: str, token: str) -> str:
    response = fetch_nfce_id(dados_id, token)
    if 'retorno' in response and 'registros' in response['retorno'] and 'registro' in response['retorno']['registros']:
        nfce_id = response['retorno']['registros']['registro']['idNotaFiscal']
        logger.info(f"NFC-e generated successfully with idNotaFiscal: {nfce_id}")
        return nfce_id
    else:
        raise ValidationError("NFCe generation response is missing expected fields.")


def process_nota_fiscal_link_retrieval(idNotafiscal: str, token: str, dados_id: str, timestamp: str, uuid_str: str, pedido_numero: str) -> dict:
    response = fetch_nota_fiscal_link(idNotafiscal, token)
    logger.info(f"Successfully fetched Nota Fiscal link payload for idNotafiscal: {idNotafiscal}")
    folder_path = FOLDER_NAME.format(timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str)
    store_payload(response, NFCe_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
        'uuid_str': uuid_str,
        'nfce_id': idNotafiscal,
        'data_type': 'nfce.link',
        'pedido_id': pedido_numero
    })
    return response


def fetch_pdv_pedido_data(dados_id: str, token: str) -> dict:
    logger.debug(f"Fetching PDV pedido data - dados_id: {dados_id}")
    return make_api_call(f"{BASE_URL}pdv.pedido.obter.php?token={token}&id={dados_id}")


def fetch_produto_data(item_id: str, token: str) -> dict:
    logger.debug(f"Fetching produto data - item_id: {item_id}")
    return make_api_call(f"{BASE_URL}produto.obter.php?token={token}&id={item_id}&formato=JSON")


def fetch_pedidos_pesquisa_data(pedido_numero: str, token: str) -> dict:
    logger.debug(f"Fetching pedidos pesquisa data - pedido_numero: {pedido_numero}")
    return make_api_call(f"{BASE_URL}pedidos.pesquisa.php?token={token}&numero={pedido_numero}&formato=JSON")


def fetch_nfce_id(dados_id: str, token: str) -> dict:
    logger.debug(f"Fetching NFC-e ID for dados_id: {dados_id}")
    url = f"{BASE_URL}gerar.nota.fiscal.pedido.php?token={token}&formato=JSON&id={dados_id}&modelo=NFCe"
    response = make_api_call(url)
    return response


def fetch_nota_fiscal_link(idNotafiscal: str, token: str) -> dict:
    logger.debug(f"Fetching Nota Fiscal link for idNotafiscal: {idNotafiscal}")
    url = f"{BASE_URL}nota.fiscal.obter.link.php?token={token}&formato=JSON&id={idNotafiscal}"
    response = make_api_call(url)
    return response


def generate_checksum(data: dict) -> str:
    logger.debug("Generating checksum")
    return hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()


def store_payload(data: dict, filename_template: str, folder_path: str, metadata: dict) -> None:
    file_path = f"{folder_path}/{FILE_PREFIX}{filename_template}.json"
    logger.debug(f"Storing payload in GCS at: {file_path}")

    checksum = generate_checksum(data)
    processing_timestamp = datetime.utcnow().isoformat() + 'Z'

    full_metadata = {}
    metadata_fields = {
        'UUID': 'uuid_str',
        'Pedido-ID': 'pedido_id',
        'Data-Type': 'data_type',
        'Processing-Timestamp': processing_timestamp,
        'Checksum': checksum,
        'Project-ID': PROJECT_ID,
        'Source-Identifier': SOURCE_IDENTIFIER,
        'Version-Control': VERSION_CONTROL
    }

    if metadata.get('data_type') == 'produto':
        metadata_fields['Produto-ID'] = 'produto_id'

    for key, value_key in metadata_fields.items():
        value = metadata.get(value_key, '') if value_key in metadata else metadata_fields[key]
        if value:
            full_metadata[key] = value

    try:
        bucket = storage_client.bucket(TARGET_BUCKET_NAME)
        blob = bucket.blob(file_path)
        blob.metadata = full_metadata
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        logger.debug(f"Payload stored with metadata: {full_metadata}")
    except Exception as e:
        logger.exception(f"Failed to store payload in GCS: {e}")


def publish_notification(topic_path: str, pdv_pedido_data: dict, produto_payloads: list, pedidos_pesquisa_data: dict, nota_fiscal_link_data: dict, timestamp: str, uuid_str: str) -> None:
    try:
        message = {
            'pdv_pedido_data': pdv_pedido_data,
            'produto_data': produto_payloads,
            'pedidos_pesquisa_data': pedidos_pesquisa_data,
            'nota_fiscal_link_data': nota_fiscal_link_data,
            'timestamp': timestamp,
            'uuid': uuid_str
        }
        serialized_message = json.dumps(message, ensure_ascii=False)
        payload = serialized_message.encode('utf-8')
        logger.info(f"Notification published to {topic_path} with message: {serialized_message}")
        future = publisher.publish(topic_path, data=payload)
        future.result()
    except Exception as e:
        logger.exception(f"Failed to publish notification: {e}")
