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
        # 1. Load Customers
        load_customers_to_bq = GCSToBigQueryOperator(
            task_id="load_src_customers_to_bq",
            bucket="olist-raw-landing-bucket",                   
            source_objects=["landing/customers/*.csv"],       
            destination_project_dataset_table="raw_olist.src_customers",
            source_format="CSV",
            write_disposition="WRITE_TRUNCATE",
            skip_leading_rows=1,                              # Skips the CSV header row
            autodetect=True,                                  # Tells BigQuery to automatically infer schemas
            gcp_conn_id="google_cloud_default",
        )

        # 2. Load Orders
        load_orders_to_bq = GCSToBigQueryOperator(
            task_id="load_src_orders_to_bq",
            bucket="olist-raw-landing-bucket",
            source_objects=["landing/orders/*.csv"],
            destination_project_dataset_table="raw_olist.src_orders",
            source_format="CSV",
            write_disposition="WRITE_TRUNCATE",
            skip_leading_rows=1,
            autodetect=True,
            gcp_conn_id="google_cloud_default",
        )

        # 3. Load Order Items
        load_order_items_to_bq = GCSToBigQueryOperator(
            task_id="load_src_order_items_to_bq",
            bucket="olist-raw-landing-bucket",
            source_objects=["landing/order_items/*.csv"],
            destination_project_dataset_table="raw_olist.src_order_items",
            source_format="CSV",
            write_disposition="WRITE_TRUNCATE",
            skip_leading_rows=1,
            autodetect=True,
            gcp_conn_id="google_cloud_default",
        )

        # 4. Load Products
        load_products_to_bq = GCSToBigQueryOperator(
            task_id="load_src_products_to_bq",
            bucket="olist-raw-landing-bucket",
            source_objects=["landing/products/*.csv"],
            destination_project_dataset_table="raw_olist.src_products",
            source_format="CSV",
            write_disposition="WRITE_TRUNCATE",
            skip_leading_rows=1,
            autodetect=True,
            gcp_conn_id="google_cloud_default",
        )

        # 5. Load Order Payments
        load_order_payments_to_bq = GCSToBigQueryOperator(
            task_id="load_src_order_payments_to_bq",
            bucket="olist-raw-landing-bucket",
            source_objects=["landing/order_payments/*.csv"],
            destination_project_dataset_table="raw_olist.src_order_payments",
            source_format="CSV",
            write_disposition="WRITE_TRUNCATE",
            skip_leading_rows=1,
            autodetect=True,
            gcp_conn_id="google_cloud_default",
        )

        # Extract from Postgres to GCS >> Load from GCS into BigQuery Warehouse
        # Extract to GCS >> Load to BigQuery Warehouse
        extract_src_customers_to_gcs >> load_customers_to_bq
        extract_src_orders_to_gcs >> load_orders_to_bq
        extract_src_order_items_to_gcs >> load_order_items_to_bq
        extract_src_products_to_gcs >> load_products_to_bq
        extract_src_order_payments_to_gcs >> load_order_payments_to_bq
