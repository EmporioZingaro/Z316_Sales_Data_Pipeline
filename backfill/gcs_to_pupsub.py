import re
import json
import logging
from time import sleep

from google.cloud import pubsub_v1, storage

BUCKET_NAME = 'z316-tiny-api'
PUBSUB_TOPIC = 'projects/emporio-zingaro/topics/api-to-gcs_DONE'
FILENAME_PATTERN = r"z316-tiny-api-\d+-(produto|pdv|pesquisa)(-\d+)?-(\d{8}T\d{6})-([a-f0-9-]+)\.json"
SLEEP_INTERVAL = 0.2

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
storage_client = storage.Client()
pubsub_publisher = pubsub_v1.PublisherClient()
bucket = storage_client.bucket(BUCKET_NAME)


def parse_filename(full_path):
    filename = full_path.split('/')[-1]
    match = re.search(FILENAME_PATTERN, filename)
    if match:
        product_type = match.group(1)
        timestamp_str = match.group(3)
        uuid = match.group(4)
        product_id = match.group(2)[1:] if product_type == 'produto' else None
        logging.debug(f"File parsed successfully: Type={product_type}, Timestamp={timestamp_str}, UUID={uuid}, ProductID={product_id}, Filename={filename}")
        return product_type, timestamp_str, uuid, product_id
    else:
        logging.error(f"Error parsing filename {filename}: Filename does not match expected pattern.")
        return None, None, None, None


def create_pubsub_message(pdv_content, pesquisa_content, produto_contents, timestamp, uuid):
    produto_data = []
    for content, _ in produto_contents:
        produto = json.loads(content)
        produto_data.append(produto)

    message = {
        "pdv_pedido_data": json.loads(pdv_content),
        "produto_data": produto_data,
        "pedidos_pesquisa_data": json.loads(pesquisa_content),
        "nota_fiscal_link_data": {"retorno": {"status_processamento": "3", "status": "OK", "link_nfe": ""}},
        "timestamp": timestamp,
        "uuid": uuid
    }
    logging.debug(f"Pub/Sub message created: {message}")
    return message


def publish_message(topic_name, message):
    try:
        serialized_message = json.dumps(message, ensure_ascii=False)
        payload = serialized_message.encode('utf-8')
        future = pubsub_publisher.publish(topic_name, data=payload)
        future.result()
        logging.info(f"Notification published to {topic_name} with message: {serialized_message}")
        sleep(SLEEP_INTERVAL)
    except Exception as e:
        logging.exception(f"Failed to publish notification: {e}")


def process_folder(folder_name):
    blobs = bucket.list_blobs(prefix=folder_name)
    pdv_content = pesquisa_content = timestamp = uuid = None
    produto_contents = []
    for blob in blobs:
        content = blob.download_as_text()
        product_type, timestamp_str, file_uuid, product_id = parse_filename(blob.name)
        if product_type == 'pdv':
            pdv_content = content
            timestamp = timestamp_str
            uuid = file_uuid
        elif product_type == 'pesquisa':
            pesquisa_content = content
        elif product_type == 'produto':
            produto_contents.append((content, product_id))
    if pdv_content and pesquisa_content and timestamp and uuid:
        message = create_pubsub_message(pdv_content, pesquisa_content, produto_contents, timestamp, uuid)
        publish_message(PUBSUB_TOPIC, message)
    else:
        logging.warning(f"Skipping folder {folder_name} due to missing essential files.")


def main():
    logging.info("Starting file processing...")
    folders = set(blob.name.split('/')[0] for blob in bucket.list_blobs())
    for folder_name in folders:
        logging.info(f"Processing folder: {folder_name}")
        process_folder(folder_name)
    logging.info("File processing completed.")


if __name__ == '__main__':
    main()
