import logging
import requests
import pandas as pd
from datetime import datetime
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from google.cloud import bigquery, secretmanager
from google.api_core.exceptions import NotFound

logging.basicConfig(level=logging.INFO)

API_BASE_URL = "https://api.tiny.com.br/api2/contatos.pesquisa.php"
SECRET_NAME = "projects/559935551835/secrets/z316-tiny-token-api/versions/latest"
BIGQUERY_DATASET = 'z316_tiny'
BIGQUERY_TABLE = 'z316-tiny-contatos'


class ValidationError(Exception):
    pass


class InvalidTokenError(Exception):
    pass


class RetryableError(Exception):
    pass


def access_secret_version(secret_name):
    """Accesses the API token from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8")


@retry(wait=wait_exponential(multiplier=2.5, min=30, max=187.5),
       stop=stop_after_attempt(4),
       retry=retry_if_exception_type((requests.exceptions.RequestException, RetryableError)))
def fetch_data_from_api(page, api_token):
    """Fetches data from API with exponential backoff on retry."""
    api_url = f"{API_BASE_URL}?token={api_token}&pagina={page}&formato=json"
    sanitized_url = api_url.split('?')[0]
    logging.info(f"Making API call to: {sanitized_url}")
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        json_data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Request to {sanitized_url} failed: {e}")
        raise
    except ValueError as e:
        logging.error(f"Decoding JSON failed: {e}")
        raise RetryableError("Failed to decode JSON, retrying...")

    validate_json_payload(json_data)

    return json_data


def validate_json_payload(json_data):
    """Validates the JSON payload from API response."""
    status = json_data.get('retorno', {}).get('status_processamento')
    if status == '3':
        return
    elif status == '2':
        raise ValidationError("Invalid query parameter.")
    elif status == '1':
        handle_error_in_response(json_data)


def handle_error_in_response(json_data):
    """Handles error in API response."""
    error_code = json_data.get('retorno', {}).get('codigo_erro')
    errors = json_data.get('retorno', {}).get('erros', [])
    error_message = errors[0]['erro'] if errors else "Unknown error"
    if error_code == '1':
        raise InvalidTokenError(f"Token is not valid: {error_message}")
    else:
        raise RetryableError(f"Error encountered, will attempt retry: {error_message}")


def transform_data(json_data):
    """Processes and transforms JSON data into a DataFrame."""
    contacts = json_data.get('retorno', {}).get('contatos', [])
    transformed_contacts = []

    for contact in contacts:
        contact_info = contact['contato']
        contact_info['data_criacao'] = datetime.strptime(
            contact_info['data_criacao'], '%d/%m/%Y %H:%M:%S')
        transformed_contacts.append(contact_info)

    return pd.DataFrame(transformed_contacts)


def update_bigquery_table(transformed_data):
    """Interacts with Google BigQuery to update the table, creating it if it doesn't exist."""
    client = bigquery.Client()
    dataset_id = f"{client.project}.{BIGQUERY_DATASET}"
    table_id = f"{dataset_id}.{BIGQUERY_TABLE}"
    
    transformed_data['data_criacao'] = pd.to_datetime(transformed_data['data_criacao']).dt.strftime('%Y-%m-%d %H:%M:%S')

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True

    try:
        client.get_dataset(dataset_id)
        logging.info(f"Dataset '{dataset_id}' already exists.")
    except NotFound:
        logging.info(f"Dataset '{dataset_id}' not found, creating it.")
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset)
        logging.info(f"Created dataset '{dataset_id}'.")

    try:
        client.get_table(table_id)
        logging.info(f"Table '{table_id}' already exists. Truncating table.")
        client.query(f"TRUNCATE TABLE `{table_id}`").result()
    except NotFound:
        logging.info(f"Table '{table_id}' not found, creating it.")
        table = bigquery.Table(table_id)
        client.create_table(table)
        logging.info(f"Created table '{table_id}'.")

    load_job = client.load_table_from_dataframe(transformed_data, table_id, job_config=job_config)
    load_job.result()
    logging.info(f"Data loaded to table '{table_id}'.")

def main(request):
    """Main function for Cloud Function entry point."""
    api_token = access_secret_version(SECRET_NAME)
    initial_data = fetch_data_from_api(1, api_token)
    if not initial_data:
        logging.error("Failed to fetch initial data from API")
        return "Failed to fetch initial data from API", 500

    all_contacts = transform_data(initial_data)

    num_pages = int(initial_data.get('retorno', {}).get('numero_paginas', 1))
    for page in range(2, num_pages + 1):
        data = fetch_data_from_api(page, api_token)
        if data:
            all_contacts = pd.concat([all_contacts, transform_data(data)], ignore_index=True)

    if not all_contacts.empty:
        update_bigquery_table(all_contacts)
    else:
        logging.info("No data to process.")

    return "Process completed successfully", 200
