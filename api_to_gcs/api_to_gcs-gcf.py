import json
import requests
import hashlib
import os
from datetime import datetime

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
SECRET_PATH = os.environ['SECRET_PATH']
PROJECT_ID = os.environ['PROJECT_ID']
SOURCE_IDENTIFIER = os.environ['SOURCE_IDENTIFIER']
VERSION_CONTROL = os.environ['VERSION_CONTROL']
PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']

storage_client = storage.Client()
secret_manager_client = secretmanager.SecretManagerServiceClient()

class ValidationError(Exception):
    pass


class InvalidTokenError(Exception):
    pass


class RetryableError(Exception):
    pass


def print_message(message: str, context: Optional[dict] = None) -> None:
    """
    Prints a message with optional context information.

    Args:
        message (str): The message to be printed.
        context (Optional[dict]): Additional context information. Defaults to None.
    """
    print(f"{message} - Context: {context}" if context else message)


def get_api_token() -> str:
    """
    Retrieves the API token from the Secret Manager.

    Returns:
        str: The API token.

    Raises:
        Exception: If there is an error accessing the secret.
    """
    try:
        print_message("Accessing API token from Secret Manager")
        response = secret_manager_client.access_secret_version(request={"name": SECRET_PATH})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print_message(f"Failed to access secret: {e}")
        raise


@retry(wait=wait_exponential(multiplier=2.5, min=30, max=187.5), stop=stop_after_attempt(4), retry=retry_if_exception_type((requests.exceptions.RequestException, RetryableError)))
def make_api_call(url: str) -> dict:
    """
    Makes an API call to the specified URL and returns the JSON response.

    Args:
        url (str): The URL to make the API call to.

    Returns:
        dict: The JSON response from the API.

    Raises:
        requests.exceptions.RequestException: If there is an error making the API request.
        ValidationError: If the JSON payload validation fails.
    """
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


def validate_json_payload(json_data: dict) -> None:
    """
    Validates the JSON payload received from the API.

    Args:
        json_data (dict): The JSON payload to validate.

    Raises:
        ValidationError: If the payload validation fails.
        InvalidTokenError: If the token is invalid.
        RetryableError: If a retryable error is encountered.
    """
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
    """
    Reads the webhook payload from the specified Google Cloud Storage bucket and file.

    Args:
        bucket_name (str): The name of the bucket containing the webhook payload.
        file_name (str): The name of the file containing the webhook payload.

    Returns:
        dict: The webhook payload as a dictionary.

    Raises:
        Exception: If there is an error reading the webhook payload.
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        return json.loads(blob.download_as_string(client=None))
    except Exception as e:
        print_message(f"Failed to read webhook payload: {e}")
        raise


def process_webhook_payload(event: dict, context: Any) -> None:
    """
    Processes the webhook payload received from the event.

    Args:
        event (dict): The event containing the webhook payload.
        context (Any): The context of the event.
    """
    print_message("Function execution started", context={'event_id': context.event_id})
    try:
        payload_details = extract_payload_details(event)
        if not payload_details:
            return

        token = get_api_token()
        pedido_numero = process_pdv_pedido_data(*payload_details, token)
        process_pedidos_pesquisa_data(*payload_details, token, pedido_numero)
        publish_notification(PUBSUB_TOPIC, payload_details[2])

    except Exception as e:
        print_message(f"Function failed: {e}", context={'event_id': context.event_id})
    print_message("Function execution completed successfully", context={'event_id': context.event_id})


def extract_payload_details(event: dict) -> Optional[Tuple[str, str, str]]:
    """
    Extracts the payload details from the event.

    Args:
        event (dict): The event containing the payload details.

    Returns:
        Optional[Tuple[str, str, str]]: A tuple containing the dados_id, timestamp, and uuid_str,
                                         or None if dados_id is not found in the webhook payload.
    """
    file_name = event['name']
    webhook_payload = read_webhook_payload(event['bucket'], file_name)
    dados_id = webhook_payload.get('dados', {}).get('id')

    if not dados_id:
        print_message("dados.id not found in webhook payload")
        return None

    parts = file_name.rstrip('.json').split('-')
    return dados_id, parts[-6], '-'.join(parts[-5:])


def process_pdv_pedido_data(dados_id: str, timestamp: str, uuid_str: str, token: str) -> str:
    """
    Processes the PDV pedido data.

    Args:
        dados_id (str): The dados_id.
        timestamp (str): The timestamp.
        uuid_str (str): The UUID string.
        token (str): The API token.

    Returns:
        str: The pedido_numero.
    """
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


def process_pedidos_pesquisa_data(dados_id: str, timestamp: str, uuid_str: str, token: str, pedido_numero: str) -> None:
    """
    Processes the pedidos pesquisa data.

    Args:
        dados_id (str): The dados_id.
        timestamp (str): The timestamp.
        uuid_str (str): The UUID string.
        token (str): The API token.
        pedido_numero (str): The pedido_numero.
    """
    folder_path = FOLDER_NAME.format(timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str)
    pedidos_data = fetch_pedidos_pesquisa_data(pedido_numero, token)
    store_payload(pedidos_data, PESQUISA_FILENAME.format(dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str), folder_path, {
        'uuid_str': uuid_str,
        'pedido_id': pedido_numero,
        'data_type': 'pedidos.pesquisa'
    })


def fetch_pdv_pedido_data(dados_id: str, token: str) -> dict:
    """
    Fetches the PDV pedido data from the API.

    Args:
        dados_id (str): The dados_id.
        token (str): The API token.

    Returns:
        dict: The PDV pedido data.
    """
    return make_api_call(f"{BASE_URL}pdv.pedido.obter.php?token={token}&id={dados_id}")


def fetch_produto_data(item_id: str, token: str) -> dict:
    """
    Fetches the produto data from the API.

    Args:
        item_id (str): The item_id.
        token (str): The API token.

    Returns:
        dict: The produto data.
    """
    return make_api_call(f"{BASE_URL}produto.obter.php?token={token}&id={item_id}&formato=JSON")


def fetch_pedidos_pesquisa_data(pedido_numero: str, token: str) -> dict:
    """
    Fetches the pedidos pesquisa data from the API.

    Args:
        pedido_numero (str): The pedido_numero.
        token (str): The API token.

    Returns:
        dict: The pedidos pesquisa data.
    """
    return make_api_call(f"{BASE_URL}pedidos.pesquisa.php?token={token}&numero={pedido_numero}&formato=JSON")


def generate_checksum(data: dict) -> str:
    """
    Generates a checksum for the given data.

    Args:
        data (dict): The data to generate the checksum for.

    Returns:
        str: The generated checksum.
    """
    return hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()


def store_payload(data: dict, filename_template: str, folder_path: str, metadata: dict) -> None:
    """
    Stores the payload in Google Cloud Storage.

    Args:
        data (dict): The payload data.
        filename_template (str): The template for the filename.
        folder_path (str): The folder path in Google Cloud Storage.
        metadata (dict): Additional metadata to store with the payload.
    """
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


def publish_notification(topic_path: str, message: str) -> None:
    """
    Publishes a notification message to the specified Pub/Sub topic.

    Args:
        topic_path (str): The path of the Pub/Sub topic.
        message (str): The message to publish.
    """
    try:
        topic_path = publisher.topic_path(PROJECT_ID, topic_path)
        future = publisher.publish(topic_path, data=message.encode('utf-8'))
        print_message(f"Notification published to {topic_path} with message: {message}")
        future.result()
    except Exception as e:
        print_message(f"Failed to publish notification: {e}")
