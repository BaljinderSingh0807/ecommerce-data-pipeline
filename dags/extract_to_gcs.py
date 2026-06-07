from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'baljinder_singh',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'olist_source_to_gcs_landing',
    default_args=default_args,
    description='Extract tables from transactional Postgres and stream into GCS and BigQuery',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # The base tables exactly as they are named in Postgres
    tables_to_extract = [
        'src_customers', 
        'src_orders', 
        'src_order_items', 
        'src_products', 
        'src_order_payments'
    ]

    BUCKET_NAME = 'olist-raw-landing-bucket'

    # This loop dynamically creates BOTH tasks for every table and links them!
    for table in tables_to_extract:
        
        # 1. Step 1: Extract from Postgres to GCS (Saving as JSON)
        extract_to_gcs = PostgresToGCSOperator(
            task_id=f'extract_{table}_to_gcs',
            postgres_conn_id='postgres_source_conn',
            sql=f"SELECT * FROM {table};",
            bucket=BUCKET_NAME,
            filename=f'raw/olist/{table}/{table}.json',
            export_format='json',
            gzip=False,
        )

        # 2. Step 2: Load from GCS into BigQuery (Matching JSON format)
        load_to_bq = GCSToBigQueryOperator(
            task_id=f'load_{table}_to_bq',
            bucket=BUCKET_NAME,
            source_objects=[f'raw/olist/{table}/{table}.json'], 
            destination_project_dataset_table=f'project-b9e827f5-d159-4d49-990.raw_olist.{table}',
            source_format='NEWLINE_DELIMITED_JSON', 
            write_disposition='WRITE_TRUNCATE',
            autodetect=True,
            gcp_conn_id='google_cloud_default',
        )

        # 3. Step 3: Establish the dependency right inside the loop!
        extract_to_gcs >> load_to_bq