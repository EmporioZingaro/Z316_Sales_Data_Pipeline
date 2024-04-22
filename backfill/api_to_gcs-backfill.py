import json
import requests
import uuid
import hashlib
import time
from datetime import datetime
from google.cloud import storage, secretmanager, pubsub_v1
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

BASE_URL = "https://api.tiny.com.br/api2/"
FILE_PREFIX = "z316-tiny-api-"
TARGET_BUCKET_NAME = "z316-tiny-api"
FOLDER_NAME = "{timestamp}-{dados_id}-{uuid_str}"
PDV_FILENAME = "{dados_id}-pdv-{timestamp}-{uuid_str}"
PESQUISA_FILENAME = "{dados_id}-pesquisa-{timestamp}-{uuid_str}"
PRODUTO_FILENAME = "{dados_id}-produto-{produto_id}-{timestamp}-{uuid_str}"
SECRET_PATH = "projects/559935551835/secrets/z316-tiny-token-api/versions/latest"
SLEEP_TIME = 3
PUBSUB_TOPIC = 'projects/emporio-zingaro/topics/api-to-gcs_DONE'
SLEEP_INTERVAL = 0.2

PROJECT_ID = "z316-sales-data-pipeline"
SOURCE_IDENTIFIER = "backfill"
VERSION_CONTROL = "git_commit_id"

storage_client = storage.Client()
secret_manager_client = secretmanager.SecretManagerServiceClient()
pubsub_publisher = pubsub_v1.PublisherClient()

class ValidationError(Exception):
    pass

class InvalidTokenError(Exception):
    pass

class RetryableError(Exception):
    pass

def print_message(message, context=None):
    print(f"{message} - Context: {context}" if context else message)

def get_api_token():
    try:
        print_message("Accessing API token from Secret Manager")
        response = secret_manager_client.access_secret_version(request={"name": SECRET_PATH})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print_message(f"Failed to access secret: {e}")
        raise

@retry(wait=wait_exponential(multiplier=2, min=60, max=1800), stop=stop_after_attempt(8), retry=retry_if_exception_type(requests.exceptions.RequestException))
def make_api_call(url):
    time.sleep(SLEEP_TIME)
    try:
        sanitized_url = url.split('?token=')[0]
        print_message(f"Making API call to: {sanitized_url}")
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

        validate_json_payload(json_data)

        return json_data
    except requests.exceptions.RequestException as e:
        print_message(f"API request failed: {e}")
        raise
    except ValidationError as e:
        print_message(f"Payload validation failed: {e}")
        raise

def validate_json_payload(json_data):
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

def generate_checksum(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()

def store_payload(data, filename_template, folder_path, metadata):
    file_path = f"{folder_path}/{FILE_PREFIX}{filename_template}.json"
    print_message(f"Storing payload in GCS at: {file_path}")

    checksum = generate_checksum(data)
    processing_timestamp = datetime.utcnow().isoformat() + 'Z'

    full_metadata = {
        'UUID': metadata.get('uuid_str', ''),
        'Pedido-ID': metadata.get('pedido_id', ''),
        'Produto-ID': metadata.get('produto_id', ''),
        'Data-Type': metadata.get('data_type', ''),
        'Processing-Timestamp': processing_timestamp,
        'Checksum': checksum,
        'Project-ID': PROJECT_ID,
        'Source-Identifier': SOURCE_IDENTIFIER,
        'Version-Control': VERSION_CONTROL
    }

    full_metadata = {k: v for k, v in full_metadata.items() if v}

    try:
        bucket = storage_client.bucket(TARGET_BUCKET_NAME)
        blob = bucket.blob(file_path)
        blob.metadata = full_metadata
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        print_message(f"Payload stored with metadata: {full_metadata}")
    except Exception as e:
        print_message(f"Failed to store payload in GCS: {e}")

def is_pedido_processed_just_in_time(dados_id):
    search_pattern = f"-{dados_id}-"
    blobs = storage_client.list_blobs(TARGET_BUCKET_NAME)
    for blob in blobs:
        if search_pattern in blob.name:
            return True
    return False

def fetch_and_extract_dados_ids():
    all_dados_ids = set()
    blobs = storage_client.list_blobs(TARGET_BUCKET_NAME)
    for blob in blobs:
        parts = blob.name.split('-')
        if len(parts) > 1:
            dados_id = parts[1]
            all_dados_ids.add(dados_id)
    return all_dados_ids

def generate_timestamp_and_uuid(data_pedido):
    timestamp = datetime.strptime(data_pedido, "%d/%m/%Y").strftime("%Y%m%dT000000")
    return timestamp, str(uuid.uuid4())

def fetch_pdv_pedido_data(dados_id, token):
    return make_api_call(f"{BASE_URL}pdv.pedido.obter.php?token={token}&id={dados_id}")

def fetch_produto_data(item_id, token):
    return make_api_call(f"{BASE_URL}produto.obter.php?token={token}&id={item_id}&formato=JSON")

def fetch_pedidos_pesquisa_data(pedido_numero, token):
    return make_api_call(f"{BASE_URL}pedidos.pesquisa.php?token={token}&numero={pedido_numero}&formato=JSON")

def create_pubsub_message(pdv_pedido_data, produto_data, pedidos_pesquisa_data, timestamp, uuid):
    message = {
        "pdv_pedido_data": pdv_pedido_data,
        "produto_data": produto_data,
        "pedidos_pesquisa_data": pedidos_pesquisa_data,
        "nota_fiscal_link_data": {"retorno": {"status_processamento": "3", "status": "OK", "link_nfe": ""}},
        "timestamp": timestamp,
        "uuid": uuid
    }
    print_message(f"Pub/Sub message created: {message}", context="create_pubsub_message")
    return message

def publish_message(topic_name, message):
    try:
        serialized_message = json.dumps(message, ensure_ascii=False)
        payload = serialized_message.encode('utf-8')
        future = pubsub_publisher.publish(topic_name, data=payload)
        future.result()
        print_message(f"Notification published to {topic_name} with message: {serialized_message}", context="publish_message")
        time.sleep(SLEEP_INTERVAL)
    except Exception as e:
        print_message(f"Failed to publish notification: {e}", context="publish_message")

def process_pdv_pedido_data(dados_id, timestamp, uuid_str, token, folder_path):
    pdv_pedido_data = fetch_pdv_pedido_data(dados_id, token)
    pedido_numero = pdv_pedido_data.get('retorno', {}).get('pedido', {}).get('numero')

    store_payload(pdv_pedido_data, PDV_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
        'uuid_str': uuid_str,
        'pedido_id': pedido_numero,
        'data_type': 'pdv.pedido'
    })

    produto_data = []
    for item in pdv_pedido_data.get('retorno', {}).get('pedido', {}).get('itens', []):
        item_id = item.get('idProduto')
        if item_id:
            produto_item_data = fetch_produto_data(item_id, token)
            store_payload(produto_item_data, PRODUTO_FILENAME.format(dados_id=dados_id, produto_id=item_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
                'uuid_str': uuid_str,
                'pedido_id': pedido_numero,
                'produto_id': item_id,
                'data_type': 'produto'
            })
            produto_data.append(produto_item_data)

    return pedido_numero, pdv_pedido_data, produto_data

def process_pedidos_pesquisa_data(dados_id, timestamp, uuid_str, token, pedido_numero, folder_path):
    pedidos_data = fetch_pedidos_pesquisa_data(pedido_numero, token)

    store_payload(pedidos_data, PESQUISA_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
        'uuid_str': uuid_str,
        'pedido_id': pedido_numero,
        'data_type': 'pedidos.pesquisa'
    })

    return pedidos_data

def process_pedido(pedido, all_processed_dados_ids, token):
    dados_id, data_pedido, pedido_numero = pedido['id'], pedido['data_pedido'], pedido['numero']

    if dados_id not in all_processed_dados_ids:
        if not is_pedido_processed_just_in_time(dados_id):
            print_message(f"Pedido {dados_id} not found in initial set, but confirmed unprocessed. Proceeding.")
            timestamp, uuid_str = generate_timestamp_and_uuid(data_pedido)
            folder_path = FOLDER_NAME.format(timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str)

            pedido_numero, pdv_pedido_data, produto_data = process_pdv_pedido_data(dados_id, timestamp, uuid_str, token, folder_path)
            pedidos_pesquisa_data = process_pedidos_pesquisa_data(dados_id, timestamp, uuid_str, token, pedido_numero, folder_path)

            pubsub_message = create_pubsub_message(pdv_pedido_data, produto_data, pedidos_pesquisa_data, timestamp, uuid_str)
            publish_message(PUBSUB_TOPIC, pubsub_message)

            all_processed_dados_ids.add(dados_id)
        else:
            print_message(f"Pedido {dados_id} confirmed processed on just-in-time check. Skipping.")
    else:
        print_message(f"Pedido {dados_id} already processed. Skipping.")

def process_pedidos(page_limit=None):
    token = get_api_token()
    pagina = 1
    numero_paginas = 1

    all_processed_dados_ids = fetch_and_extract_dados_ids()

    while pagina <= numero_paginas:
        url = f"{BASE_URL}pedidos.pesquisa.php?token={token}&formato=JSON&pagina={pagina}"
        pedidos_data = make_api_call(url)
        numero_paginas = int(pedidos_data.get('retorno', {}).get('numero_paginas', 1))

        for pedido in pedidos_data.get('retorno', {}).get('pedidos', []):
            process_pedido(pedido['pedido'], all_processed_dados_ids, token)

        pagina += 1
        if page_limit and pagina > page_limit:
            break

if __name__ == "__main__":
    process_pedidos()
