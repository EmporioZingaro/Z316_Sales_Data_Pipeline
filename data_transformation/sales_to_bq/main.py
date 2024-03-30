import os
import json
import base64
import logging
from datetime import datetime

from google.cloud import bigquery
from google.api_core import retry

logging.basicConfig(level=logging.INFO)
client = bigquery.Client()

PEDIDOS_TABLE_ID = os.environ.get('PEDIDOS_TABLE_ID')
ITENS_PEDIDO_TABLE_ID = os.environ.get('ITENS_PEDIDO_TABLE_ID')
SOURCE_ID = os.environ.get('SOURCE_ID')

pedidos_schema = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "STRING"),
    bigquery.SchemaField("pedido_dia", "DATE"),
    bigquery.SchemaField("pedido_id", "STRING"),
    bigquery.SchemaField("pedido_numero", "STRING"),
    bigquery.SchemaField("cliente_nome", "STRING"),
    bigquery.SchemaField("cliente_cpf", "STRING"),
    bigquery.SchemaField("cliente_email", "STRING"),
    bigquery.SchemaField("cliente_celular", "STRING"),
    bigquery.SchemaField("vendedor_nome", "STRING"),
    bigquery.SchemaField("vendedor_id", "STRING"),
    bigquery.SchemaField("valor_produtos_custo", "FLOAT"),
    bigquery.SchemaField("valor_produtos_sem_desconto", "FLOAT"),
    bigquery.SchemaField("desconto_produtos", "FLOAT"),
    bigquery.SchemaField("desconto_pedido", "FLOAT"),
    bigquery.SchemaField("desconto_total", "FLOAT"),
    bigquery.SchemaField("valor_faturado", "FLOAT"),
    bigquery.SchemaField("valor_lucro", "FLOAT"),
    bigquery.SchemaField("forma_pagamento", "STRING"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("processed_timestamp", "TIMESTAMP")
]

itens_pedido_schema = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "STRING"),
    bigquery.SchemaField("pedido_dia", "DATE"),
    bigquery.SchemaField("pedido_id", "STRING"),
    bigquery.SchemaField("pedido_numero", "STRING"),
    bigquery.SchemaField("cliente_nome", "STRING"),
    bigquery.SchemaField("cliente_cpf", "STRING"),
    bigquery.SchemaField("cliente_email", "STRING"),
    bigquery.SchemaField("cliente_celular", "STRING"),
    bigquery.SchemaField("vendedor_nome", "STRING"),
    bigquery.SchemaField("vendedor_id", "STRING"),
    bigquery.SchemaField("produto_id", "STRING"),
    bigquery.SchemaField("produto_nome", "STRING"),
    bigquery.SchemaField("produto_categoria_principal", "STRING"),
    bigquery.SchemaField("produto_categoria_secundaria", "STRING"),
    bigquery.SchemaField("produto_valor_custo_und", "FLOAT"),
    bigquery.SchemaField("produto_valor_sem_desconto_und", "FLOAT"),
    bigquery.SchemaField("produto_valor_com_desconto_und", "FLOAT"),
    bigquery.SchemaField("produto_valor_lucro_und", "FLOAT"),
    bigquery.SchemaField("desconto_produto_und", "FLOAT"),
    bigquery.SchemaField("desconto_pedido_und", "FLOAT"),
    bigquery.SchemaField("desconto_total_und", "FLOAT"),
    bigquery.SchemaField("produto_quantidade", "FLOAT"),
    bigquery.SchemaField("desconto_produto", "FLOAT"),
    bigquery.SchemaField("desconto_pedido", "FLOAT"),
    bigquery.SchemaField("desconto_total", "FLOAT"),
    bigquery.SchemaField("total_produto_valor_custo", "FLOAT"),
    bigquery.SchemaField("total_produto_valor_sem_desconto", "FLOAT"),
    bigquery.SchemaField("total_produto_valor_faturado", "FLOAT"),
    bigquery.SchemaField("total_produto_valor_lucro", "FLOAT"),
    bigquery.SchemaField("forma_pagamento", "STRING"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("processed_timestamp", "TIMESTAMP")
]


def transform_date_format(date_str: str) -> str:
    logging.debug(f"Transforming date format for: {date_str}")
    try:
        transformed_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        logging.info(f"Transformed date: {transformed_date}")
        return transformed_date
    except ValueError as e:
        logging.error(f"Error transforming date format: {e}, returning original date string.")
        return date_str


def calculate_total_product_cost(produto_data, pdv_pedido_data):
    logging.debug("Calculating total product cost.")
    total_cost = 0
    for item in pdv_pedido_data['itens']:
        produto_id = item['idProduto']
        produto = next((p for p in produto_data if p['retorno']['produto']['id'] == produto_id), None)
        if produto:
            preco_custo = float(produto['retorno']['produto']['preco_custo'])
            quantidade = float(item['quantidade'])
            total_cost += preco_custo * quantidade
    logging.info(f"Total product cost: {total_cost}")
    return total_cost


def calculate_total_product_value_without_discount(pdv_pedido_data):
    logging.debug("Calculating total product value without discount.")
    total_value = 0
    for item in pdv_pedido_data['itens']:
        valor_sem_desconto = float(item['valor']) / (1 - float(item['desconto']) / 100)
        total_value += valor_sem_desconto
    logging.info(f"Total product value without discount: {total_value}")
    return total_value


def calculate_total_product_value_with_discount(pdv_pedido_data):
    total_value = 0
    for item in pdv_pedido_data['itens']:
        total_value += float(item['valor'])
    return total_value


def calculate_pedido_discount(desconto_pedido_str, total_produtos):
    logging.debug(f"Calculating pedido discount from string: {desconto_pedido_str}")
    try:
        if '%' in desconto_pedido_str:
            desconto_pedido = float(desconto_pedido_str.replace('%', '')) / 100 * float(total_produtos)
        else:
            desconto_pedido = float(desconto_pedido_str.replace(',', '.'))
        desconto_pedido = max(0, desconto_pedido)
        logging.info(f"Calculated pedido discount: {desconto_pedido}")
        return desconto_pedido
    except ValueError as e:
        logging.error(f"Error calculating pedido discount: {e}, returning 0.")
        return 0.0


def calculate_total_pre_discount_value(pdv_pedido_data):
    total_pre_discount_value = 0
    for item in pdv_pedido_data['itens']:
        total_pre_discount_value += float(item['valor']) * float(item['quantidade'])
    return total_pre_discount_value


def extract_total_discount(pdv_pedido_data):
    logging.debug("Extracting total discount from pdv_pedido_data.")
    desconto = pdv_pedido_data['desconto']
    try:
        if '%' in desconto:
            total_discount = max(0, float(desconto.replace('%', '')) / 100 * float(pdv_pedido_data['totalProdutos']))
        else:
            total_discount = max(0, float(desconto.replace(',', '.')))
        logging.info(f"Extracted total discount: {total_discount}")
        return total_discount
    except ValueError as e:
        logging.error(f"Error extracting total discount: {e}, returning 0.")
        return 0.0


def calculate_proportional_discount(item_pre_discount_value, total_pre_discount_value, total_discount):
    contribution_percentage = item_pre_discount_value / total_pre_discount_value
    proportional_discount = total_discount * contribution_percentage
    return proportional_discount


def calculate_item_discount(item_valor, item_desconto):
    item_discount = item_valor / (1 - item_desconto / 100) - item_valor
    return item_discount


def process_item(item, produto_data, pdv_pedido_data, total_pre_discount_value, total_discount):
    produto_id = item['idProduto']
    produto = next((p for p in produto_data if p['retorno']['produto']['id'] == produto_id), None)
    if produto:
        produto_nome = item['descricao']
        preco_custo = float(produto['retorno']['produto']['preco_custo'])
        quantidade = float(item['quantidade'])
        produto_valor_custo_und = preco_custo
        produto_valor_custo = produto_valor_custo_und * quantidade
        categoria = produto['retorno']['produto']['categoria']
        split_index = categoria.find(' >> ')
        if split_index != -1:
            produto_categoria_principal = categoria[:split_index].strip()
            produto_categoria_secundaria = categoria[split_index + len(' >> '):].strip()
        else:
            produto_categoria_principal = categoria
            produto_categoria_secundaria = ''
        produto_quantidade = float(item['quantidade'])
        valor_sem_desconto_und = float(item['valor']) / (1 - float(item['desconto']) / 100)
        desconto_produto_und = calculate_item_discount(float(item['valor']), float(item['desconto']))
        desconto_produto = desconto_produto_und * produto_quantidade
        item_pre_discount_value = float(item['valor']) * float(item['quantidade'])
        desconto_pedido = calculate_proportional_discount(item_pre_discount_value, total_pre_discount_value, total_discount)
        desconto_pedido_und = desconto_pedido / produto_quantidade
        desconto_total_und = desconto_produto_und + desconto_pedido_und
        desconto_total = desconto_produto + desconto_pedido
        valor_com_desconto_und = valor_sem_desconto_und - desconto_produto_und - desconto_pedido_und
        produto_valor_lucro_und = valor_com_desconto_und - produto_valor_custo_und
        total_produto_valor_sem_desconto = valor_sem_desconto_und * produto_quantidade
        total_produto_valor_faturado = valor_com_desconto_und * produto_quantidade
        total_produto_valor_custo = produto_valor_custo_und * produto_quantidade
        total_produto_valor_lucro = total_produto_valor_faturado - total_produto_valor_custo
        return {
            'produto_id': produto_id,
            'produto_nome': produto_nome,
            'produto_valor_custo_und': produto_valor_custo_und,
            'produto_categoria_principal': produto_categoria_principal,
            'produto_categoria_secundaria': produto_categoria_secundaria,
            'produto_quantidade': produto_quantidade,
            'valor_sem_desconto_und': valor_sem_desconto_und,
            'valor_com_desconto_und': valor_com_desconto_und,
            'produto_valor_lucro_und': produto_valor_lucro_und,
            'desconto_produto_und': desconto_produto_und,
            'desconto_produto': desconto_produto,
            'desconto_pedido_und': desconto_pedido_und,
            'desconto_pedido': desconto_pedido,
            'desconto_total_und': desconto_total_und,
            'desconto_total': desconto_total,
            'total_produto_valor_sem_desconto': total_produto_valor_sem_desconto,
            'total_produto_valor_faturado': total_produto_valor_faturado,
            'total_produto_valor_custo': total_produto_valor_custo,
            'total_produto_valor_lucro': total_produto_valor_lucro
        }
    return None


def create_pedidos_row(uuid, timestamp, pedido_dia, pedido_id, pedido_numero, cliente_nome, cliente_cpf, cliente_email,
                       cliente_celular, vendedor_nome, vendedor_id, valor_produtos_custo, valor_produtos_sem_desconto,
                       total_desconto_produtos, desconto_pedido, desconto_total, valor_faturado, valor_lucro,
                       forma_pagamento, source_id, processed_timestamp):
    return [
        uuid,
        timestamp,
        pedido_dia,
        pedido_id,
        pedido_numero,
        cliente_nome,
        cliente_cpf,
        cliente_email,
        cliente_celular,
        vendedor_nome,
        vendedor_id,
        valor_produtos_custo,
        valor_produtos_sem_desconto,
        total_desconto_produtos,
        desconto_pedido,
        desconto_total,
        valor_faturado,
        valor_lucro,
        forma_pagamento,
        source_id,
        processed_timestamp
    ]


def create_itens_pedido_row(uuid, timestamp, pedido_dia, pedido_numero, pedido_id, cliente_nome, cliente_cpf,
                            cliente_email, cliente_celular, vendedor_nome, vendedor_id, item_data, forma_pagamento,
                            source_id, processed_timestamp):
    return [
        uuid,
        timestamp,
        pedido_dia,
        pedido_id,
        pedido_numero,
        cliente_nome,
        cliente_cpf,
        cliente_email,
        cliente_celular,
        vendedor_nome,
        vendedor_id,
        item_data['produto_id'],
        item_data['produto_nome'],
        item_data['produto_categoria_principal'],
        item_data['produto_categoria_secundaria'],
        item_data['produto_valor_custo_und'],
        item_data['valor_sem_desconto_und'],
        item_data['valor_com_desconto_und'],
        item_data['produto_valor_lucro_und'],
        item_data['desconto_produto_und'],
        item_data['desconto_pedido_und'],
        item_data['desconto_total_und'],
        item_data['produto_quantidade'],
        item_data['desconto_produto'],
        item_data['desconto_pedido'],
        item_data['desconto_total'],
        item_data['total_produto_valor_custo'],
        item_data['total_produto_valor_sem_desconto'],
        item_data['total_produto_valor_faturado'],
        item_data['total_produto_valor_lucro'],
        forma_pagamento,
        source_id,
        processed_timestamp
    ]


def create_table_if_not_exists(table_id, schema, partition_field, clustering_fields):
    try:
        table = client.get_table(table_id)
        logging.info(f"Table {table_id} already exists.")
    except Exception as e:
        logging.info(f"Table {table_id} does not exist. Creating table with schema: {schema}")
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
        table.clustering_fields = clustering_fields
        table = client.create_table(table)
        logging.info(f"Table {table_id} created successfully.")


def insert_rows_to_table(table_id, rows):
    table = client.get_table(table_id)
    errors = client.insert_rows(table, rows)
    if errors:
        raise Exception(f"Error inserting rows into {table_id}: {errors}")
    else:
        logging.info(f"Rows inserted into {table_id} successfully.")


def process_pubsub_message(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_data = json.loads(pubsub_message)
    logging.debug(f"Received message data: {message_data}")
    uuid = message_data['uuid']
    timestamp = message_data['timestamp']
    pdv_pedido_data = message_data['pdv_pedido_data']['retorno']['pedido']
    produto_data = message_data['produto_data']
    pedidos_pesquisa_data = message_data['pedidos_pesquisa_data']['retorno']['pedidos'][0]['pedido']
    pedido_dia = transform_date_format(pdv_pedido_data['data'])
    pedido_numero = pdv_pedido_data['numero']
    pedido_id = pdv_pedido_data['id']
    cliente_nome = pdv_pedido_data['contato']['nome']
    cliente_cpf = pdv_pedido_data['contato']['cpfCnpj']
    cliente_email = pdv_pedido_data['contato']['email']
    cliente_celular = pdv_pedido_data['contato']['celular']
    vendedor_nome = pedidos_pesquisa_data['nome_vendedor']
    vendedor_id = pedidos_pesquisa_data['id_vendedor']
    valor_produtos_custo = calculate_total_product_cost(produto_data, pdv_pedido_data)
    valor_produtos_sem_desconto = calculate_total_product_value_without_discount(pdv_pedido_data)
    valor_faturado = float(pdv_pedido_data['totalVenda'])
    valor_lucro = valor_faturado - valor_produtos_custo
    desconto_pedido = calculate_pedido_discount(pdv_pedido_data['desconto'], pdv_pedido_data['totalProdutos'])
    forma_pagamento = pdv_pedido_data['formaPagamento']
    total_pre_discount_value = calculate_total_pre_discount_value(pdv_pedido_data)
    total_discount = extract_total_discount(pdv_pedido_data)
    total_desconto_produtos = 0
    itens_pedido_rows = []
    processed_timestamp = datetime.utcnow()
    for item in pdv_pedido_data['itens']:
        item_data = process_item(item, produto_data, pdv_pedido_data, total_pre_discount_value, total_discount)
        if item_data:
            total_desconto_produtos += item_data['desconto_produto']
            itens_pedido_row = create_itens_pedido_row(uuid, timestamp, pedido_dia, pedido_numero, pedido_id, cliente_nome,
                                                       cliente_cpf, cliente_email, cliente_celular, vendedor_nome,
                                                       vendedor_id, item_data, forma_pagamento, SOURCE_ID, processed_timestamp)
            itens_pedido_rows.append(itens_pedido_row)
    desconto_total = total_desconto_produtos + desconto_pedido
    pedidos_row = create_pedidos_row(uuid, timestamp, pedido_dia, pedido_id, pedido_numero, cliente_nome, cliente_cpf,
                                     cliente_email, cliente_celular, vendedor_nome, vendedor_id, valor_produtos_custo,
                                     valor_produtos_sem_desconto, total_desconto_produtos, desconto_pedido, desconto_total,
                                     valor_faturado, valor_lucro, forma_pagamento, SOURCE_ID, processed_timestamp)
    pedidos_clustering_fields = ["pedido_id", "cliente_cpf", "vendedor_id", "forma_pagamento"]
    create_table_if_not_exists(PEDIDOS_TABLE_ID, pedidos_schema, "pedido_dia", pedidos_clustering_fields)
    itens_pedido_clustering_fields = ["pedido_id", "produto_id", "cliente_cpf", "vendedor_id"]
    create_table_if_not_exists(ITENS_PEDIDO_TABLE_ID, itens_pedido_schema, "pedido_dia", itens_pedido_clustering_fields)
    insert_rows_to_table(PEDIDOS_TABLE_ID, [pedidos_row])
    insert_rows_to_table(ITENS_PEDIDO_TABLE_ID, itens_pedido_rows)
