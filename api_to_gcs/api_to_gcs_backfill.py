import json
import requests
import uuid
import time
from datetime import datetime
from google.cloud import storage, secretmanager
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

BASE_URL = "https://api.tiny.com.br/api2/"
FILE_PREFIX = "z316-tiny-api-"
TARGET_BUCKET_NAME = "z316-test-bucket"
FOLDER_NAME = "{timestamp}-{dados_id}-{uuid_str}"
PDV_FILENAME = "{dados_id}-pdv-{timestamp}-{uuid_str}"
PESQUISA_FILENAME = "{dados_id}-pesquisa-{timestamp}-{uuid_str}"
PRODUTO_FILENAME = "{dados_id}-produto-{produto_id}-{timestamp}-{uuid_str}"
SECRET_PATH = "projects/559935551835/secrets/z316-tiny-token-api/versions/latest"
SLEEP_TIME = 2

storage_client = storage.Client()
secret_manager_client = secretmanager.SecretManagerServiceClient()

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

class ValidationError(Exception):
    pass

class InvalidTokenError(Exception):
    pass

class RetryableError(Exception):
    pass

def store_payload(data, filename_template, folder_path):
    file_path = f"{folder_path}/{FILE_PREFIX}{filename_template}.json"
    print_message(f"Storing payload in GCS at: {file_path}")
    try:
        bucket = storage_client.bucket(TARGET_BUCKET_NAME)
        bucket.blob(file_path).upload_from_string(json.dumps(data), content_type='application/json')
    except Exception as e:
        print_message(f"Failed to store payload in GCS: {e}")

def is_pedido_processed(dados_id, timestamp):
    prefix = f"{timestamp}-{dados_id}-"
    blobs = storage_client.list_blobs(TARGET_BUCKET_NAME, prefix=prefix, max_results=1)
    return any(blobs)

def process_pedido(pedido, token):
    dados_id, data_pedido, pedido_numero = pedido['id'], pedido['data_pedido'], pedido['numero']
    timestamp, uuid_str = generate_timestamp_and_uuid(data_pedido)
    folder_path = FOLDER_NAME.format(timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str)

    if is_pedido_processed(dados_id, timestamp):
        print_message(f"Pedido {dados_id} already processed. Skipping.")
        return

    process_pdv_pedido_data(dados_id, timestamp, uuid_str, token, folder_path)
    process_pedidos_pesquisa_data(dados_id, timestamp, uuid_str, token, pedido_numero, folder_path)

def generate_timestamp_and_uuid(data_pedido):
    timestamp = datetime.strptime(data_pedido, "%d/%m/%Y").strftime("%Y%m%dT000000")
    return timestamp, str(uuid.uuid4())

def process_pdv_pedido_data(dados_id, timestamp, uuid_str, token, folder_path):
    pdv_pedido_data = fetch_pdv_pedido_data(dados_id, token)
    store_payload(pdv_pedido_data, PDV_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path)

    for item in pdv_pedido_data.get('retorno', {}).get('pedido', {}).get('itens', []):
        item_id = item.get('idProduto')
        if item_id:
            produto_data = fetch_produto_data(item_id, token)
            store_payload(produto_data, PRODUTO_FILENAME.format(dados_id=dados_id, produto_id=item_id, timestamp=timestamp, uuid_str=uuid_str), folder_path)

def process_pedidos_pesquisa_data(dados_id, timestamp, uuid_str, token, pedido_numero, folder_path):
    pedidos_data = fetch_pedidos_pesquisa_data(pedido_numero, token)
    store_payload(pedidos_data, PESQUISA_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path)

def fetch_pdv_pedido_data(dados_id, token):
    return make_api_call(f"{BASE_URL}pdv.pedido.obter.php?token={token}&id={dados_id}")

def fetch_produto_data(item_id, token):
    return make_api_call(f"{BASE_URL}produto.obter.php?token={token}&id={item_id}&formato=JSON")

def fetch_pedidos_pesquisa_data(pedido_numero, token):
    return make_api_call(f"{BASE_URL}pedidos.pesquisa.php?token={token}&numero={pedido_numero}&formato=JSON")

def process_pedidos(page_limit=None):
    token = get_api_token()
    pagina = 1
    numero_paginas = 1

    while pagina <= numero_paginas:
        url = f"{BASE_URL}pedidos.pesquisa.php?token={token}&formato=JSON&pagina={pagina}"
        pedidos_data = make_api_call(url)
        numero_paginas = int(pedidos_data.get('retorno', {}).get('numero_paginas', 1))

        for pedido in pedidos_data.get('retorno', {}).get('pedidos', []):
            process_pedido(pedido['pedido'], token)

        pagina += 1
        if page_limit and pagina > page_limit:
            break

if __name__ == "__main__":
    process_pedidos()
