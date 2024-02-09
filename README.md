# Z316 Sales Data Pipeline

## Overview
This repository hosts the "Z316 Sales Data Pipeline," a custom-built data management solution for Empório Zingaro, a small business in Brasília, Brasil. Developed in-house, this pipeline facilitates the extraction of sales data from TinyERP to enhance business intelligence efforts through Google Looker Studio. The pipeline is tailored to fit the unique data workflow of Empório Zingaro, utilizing Google Cloud technologies for efficient data handling.

For further details on TinyERP, explore [TinyERP](https://tiny.com.br/).

## Table of Contents
- [Overview](#overview)
- [Pipeline Architecture](#pipeline-architecture)
- [Directory Structure](#directory-structure)
- [Cloud Functions](#cloud-functions)
  - [Webhook Handler](#webhook-handler)
  - [API to GCS](#api-to-gcs)
  - [GCS to BQ](#gcs-to-bq)
- [Backfilling Scripts](#backfilling-scripts)
- [Running the Pipeline](#running-the-pipeline)
- [Future Improvements](#future-improvements)
- [Contact](#contact)

## Pipeline Architecture
The Z316 sales data pipeline is architected to capture Tiny ERP webhook notifications, process the data through a series of Google Cloud Functions, and finally persist the data into Google BigQuery for analysis. The flow operates as follows:

1. **Webhook Handler**: Tiny ERP sends a webhook notification to our Google Cloud Function `webhook_handler`, which promptly stores the incoming data into the `z316-tiny-webhook` bucket on Google Cloud Storage (GCS).

2. **API to GCS**: Upon the arrival of new JSON files in the `z316-tiny-webhook` bucket, the `api_to_gcs` Cloud Function is invoked. This function performs API calls to `pdv.pedido.pesquisa`, `pedidos.pesquisa`, and `produto.obter`, and places the response payloads into the `z316-tiny-api` bucket.

3. **GCS to BQ**: Addition of new files to the `z316-tiny-api` bucket triggers the `gcs_to_bq` Cloud Function. This function is responsible for the transportation of the data into three distinct BigQuery tables: `pdv`, `pesquisa`, and `produto`, each corresponding to the JSON type they represent.

![Data Flow Diagram](https://i.imgur.com/KgvedE5.jpg)

The included diagram visualizes the entire data flow process.

## Directory Structure
```
z316_sales_data_pipeline/
├── api_to_gcs
│ ├── api_to_gcs_backfill.py
│ └── api_to_gcs_gcf.py
├── gcs_to_bq
│ ├── gcs_to_bq_backfill.py
│ ├── gcs_to_bq-gcf_pdv.py
│ ├── gcs_to_bq-gcf_pesquisa.py
│ └── gsc_to_bq-gcf_produto.py
├── README.md
└── webhook_handler
└── vendas_webhook_handler_gcf.py
```

## Cloud Functions

### Webhook Handler
The `webhook_handler` is the entry point of our pipeline, receiving JSON payloads from Tiny ERP and storing them in the designated GCS bucket.

### API to GCS
The `api_to_gcs` set of functions call external APIs using the data from webhook payloads as parameters, capturing additional details required for our sales data analysis.

### GCS to BQ
The `gcs_to_bq` functions orchestrate the transfer of data from GCS to BigQuery, ensuring that each JSON type is correctly mapped to its respective BigQuery table.

## Backfilling Scripts
We employ backfilling scripts for the two main roles to incorporate historical sales data into our pipeline. These scripts mirror the logic of the Cloud Functions but are specifically designed to process data before the implementation of the webhook-triggered pipeline.

## Running the Pipeline
To initiate the pipeline, simply upload the JSON payload from Tiny ERP to the `z316-tiny-webhook` GCS bucket. The subsequent functions will be triggered automatically, processing the data through the pipeline and into BigQuery.

## Future Improvements
Our roadmap for enhancing the Z316 Sales Data Pipeline includes the following items:

- **Payload Variables**: List and document the variables extracted from each payload and how they are used to make the API calls.
- **Data Validation**: Outline the data validation measures present at each step of the pipeline and plan improvements for more robust validation.
- **Validation Enhancements**: Seek to enhance the overall data validation process to ensure data quality and integrity.
- **Logging Standardization**: Standardize logging methods and messages across the entire pipeline for consistency and easier debugging.
- **Data Privacy and Security**: As an in-house developed solution, special attention has been given to the privacy and security of client data. Future iterations will continue to prioritize robust security measures to maintain the confidentiality and integrity of the data throughout the pipeline. This will involve regular audits, adopting best practices in cloud security, and ensuring compliance with relevant data protection laws.

These improvements aim to refine the pipeline's efficiency, reliability, and maintainability.

## Contributing
We welcome contributions to the Z316 Sales Data Pipeline project. Your insights and improvements can help us leverage our sales data more effectively.

## Contact
For more information or to discuss contributions, please reach out to Rodrigo Brunale at [rodrigo@brunale.com](mailto:rodrigo@brunale.com).

