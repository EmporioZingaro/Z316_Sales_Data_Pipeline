import json
import requests
import hashlib
from datetime import datetime
from google.cloud import storage, secretmanager
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

BASE_URL = "https://api.tiny.com.br/api2/"
FILE_PREFIX = "z316-tiny-api-"
TARGET_BUCKET_NAME = "z316-tiny-api"
FOLDER_NAME = "{timestamp}-{dados_id}-{uuid_str}"
PDV_FILENAME = "{dados_id}-pdv-{timestamp}-{uuid_str}"
PESQUISA_FILENAME = "{dados_id}-pesquisa-{timestamp}-{uuid_str}"
PRODUTO_FILENAME = "{dados_id}-produto-{produto_id}-{timestamp}-{uuid_str}"
SECRET_PATH = "projects/559935551835/secrets/z316-tiny-token-api/versions/latest"

PROJECT_ID = "z316-sales-data-pipeline"
SOURCE_IDENTIFIER = "google-cloud-function"
VERSION_CONTROL = "git_comit_id"

storage_client = storage.Client()
secret_manager_client = secretmanager.SecretManagerServiceClient()

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

@retry(wait=wait_exponential(multiplier=2.5, min=30, max=187.5), stop=stop_after_attempt(4), retry=retry_if_exception_type((requests.exceptions.RequestException, RetryableError)))
def make_api_call(url):
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

def read_webhook_payload(bucket_name, file_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        return json.loads(blob.download_as_string(client=None))
    except Exception as e:
        print_message(f"Failed to read webhook payload: {e}")
        raise

def process_webhook_payload(event, context):
    print_message("Function execution started", context={'event_id': context.event_id})
    try:
        payload_details = extract_payload_details(event)
        if not payload_details:
            return

        token = get_api_token()
        pedido_numero = process_pdv_pedido_data(*payload_details, token)
        process_pedidos_pesquisa_data(*payload_details, token, pedido_numero)

    except Exception as e:
        print_message(f"Function failed: {e}", context={'event_id': context.event_id})
    print_message("Function execution completed successfully", context={'event_id': context.event_id})

def extract_payload_details(event):
    file_name = event['name']
    webhook_payload = read_webhook_payload(event['bucket'], file_name)
    dados_id = webhook_payload.get('dados', {}).get('id')

    if not dados_id:
        print_message("dados.id not found in webhook payload")
        return None

    parts = file_name.rstrip('.json').split('-')
    return dados_id, parts[-6], '-'.join(parts[-5:])

def process_pdv_pedido_data(dados_id, timestamp, uuid_str, token):
    folder_path = FOLDER_NAME.format(timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str)
    pdv_pedido_data = fetch_pdv_pedido_data(dados_id, token)
    pedido_numero = pdv_pedido_data.get('retorno', {}).get('pedido', {}).get('numero')
    store_payload(pdv_pedido_data, PDV_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
        'uuid_str': uuid_str,
        'pedido_id': pedido_numero,
        'data_type': 'pdv.pedido'
    })

    for item in pdv_pedido_data.get('retorno', {}).get('pedido', {}).get('itens', []):
        item_id = item.get('idProduto')
        if item_id:
            produto_data = fetch_produto_data(item_id, token)
            store_payload(produto_data, PRODUTO_FILENAME.format(dados_id=dados_id, produto_id=item_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
                'uuid_str': uuid_str,
                'pedido_id': pedido_numero,
                'produto_id': item_id,
                'data_type': 'produto'
            })

    return pedido_numero

def process_pedidos_pesquisa_data(dados_id, timestamp, uuid_str, token, pedido_numero):
    folder_path = FOLDER_NAME.format(timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str)
    pedidos_data = fetch_pedidos_pesquisa_data(pedido_numero, token)
    store_payload(pedidos_data, PESQUISA_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
        'uuid_str': uuid_str,
        'pedido_id': pedido_numero,
        'data_type': 'pedidos.pesquisa'
    })

def fetch_pdv_pedido_data(dados_id, token):
    return make_api_call(f"{BASE_URL}pdv.pedido.obter.php?token={token}&id={dados_id}")

def fetch_produto_data(item_id, token):
    return make_api_call(f"{BASE_URL}produto.obter.php?token={token}&id={item_id}&formato=JSON")

def fetch_pedidos_pesquisa_data(pedido_numero, token):
    return make_api_call(f"{BASE_URL}pedidos.pesquisa.php?token={token}&numero={pedido_numero}&formato=JSON")

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
