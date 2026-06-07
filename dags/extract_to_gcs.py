from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

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
    description='Extract tables from transactional Postgres and stream into GCS Raw Zone',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define the tables we want to extract from Postgres
    tables_to_extract = [
        'src_customers', 
        'src_orders', 
        'src_order_items', 
        'src_products', 
        'src_order_payments'
    ]

    for table in tables_to_extract:
        # Loop dynamically creates a separate Airflow Task for each table!
        extract_task = PostgresToGCSOperator(
            task_id=f'extract_{table}_to_gcs',
            postgres_conn_id='postgres_source_conn',  # We configure this connection ID in the Airflow UI
            sql=f"SELECT * FROM {table};",
            bucket='olist-raw-landing-bucket',  # Update this to your real bucket name
            filename=f'raw/olist/{table}/{table}.json',
            export_format='json',
            gzip=False,
        )