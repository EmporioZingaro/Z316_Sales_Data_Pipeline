import json
import google.cloud.storage as storage
from datetime import datetime, timezone
import uuid
from flask import abort, make_response, jsonify
from tenacity import retry, wait_exponential, stop_after_attempt, before_log
import logging
import sys

BUCKET_NAME = "z316-tiny-webhook"
FILENAME_FORMAT = "vendas/z316-tiny-webhook-vendas-{dados_id}-{timestamp}-{unique_id}.json"

logger = logging.getLogger()
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

storage_client = storage.Client()

@retry(wait=wait_exponential(multiplier=1, max=10), stop=stop_after_attempt(3), before=before_log(logger, logging.DEBUG))
def upload_to_gcs(blob, data):
    blob.upload_from_string(data, content_type='application/json')
    logger.info(f"Successfully uploaded {blob.name}")

def validate_payload(request_data):
    required_fields = ["versao", "cnpj", "tipo", "dados"]
    if not all(field in request_data for field in required_fields):
        raise ValueError("Payload missing required fields")
    if request_data["tipo"] != "inclusao_pedido":
        raise ValueError("Payload 'tipo' is not 'inclusao_pedido'")

def generate_filename(dados_id, timestamp, unique_id):
    return FILENAME_FORMAT.format(dados_id=dados_id, timestamp=timestamp, unique_id=unique_id)

def erp_webhook_handler(request):
    if request.method != 'POST':
        return make_response('Method not allowed', 405)

    if not request.data:
        return make_response('No payload found', 400)

    try:
        request_data = request.get_json(force=True)
        validate_payload(request_data)
    except ValueError as e:
        if str(e) == "Payload 'tipo' is not 'inclusao_pedido'":
            logger.info(f"Ignored payload: {e}")
            return jsonify(message=f"Ignored payload: {e}"), 200
        else:
            logger.error(f"Invalid payload: {e}")
            abort(400, description=f"Invalid payload: {e}")

    dados_id = request_data.get("dados", {}).get("id", "unknown")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    unique_id = uuid.uuid4()
    filename = generate_filename(dados_id, timestamp, unique_id)

    blob = storage_client.bucket(BUCKET_NAME).blob(filename)
    data = json.dumps(request_data)

    try:
        upload_to_gcs(blob, data)
    except Exception as e:
        logger.error(f"Failed to upload {filename} after retries: {e}")
        abort(500, description=f"Failed to upload {filename} after retries")

    return jsonify(message=f"Payload stored in {filename}", filename=filename), 200
