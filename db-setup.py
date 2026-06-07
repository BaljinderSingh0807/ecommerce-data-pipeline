import os
import pandas as pd
from sqlalchemy import create_engine

# 1. Connection Configurations (Matching your docker-compose.yaml exactly)
DB_USER = "olist_admin"
DB_PASSWORD = "olist_password_2026"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "olist_ecommerce"

# Establish connection engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 2. Directory where your Olist CSV files live
CSV_DIRECTORY = "./archive" 

# Mapping files to their new SQL table names
olist_tables = {
    "olist_customers_dataset.csv": "src_customers",
    "olist_geolocation_dataset.csv": "src_geolocation",
    "olist_order_items_dataset.csv": "src_order_items",
    "olist_order_payments_dataset.csv": "src_order_payments",
    "olist_order_reviews_dataset.csv": "src_order_reviews",
    "olist_orders_dataset.csv": "src_orders",
    "olist_products_dataset.csv": "src_products",
    "olist_sellers_dataset.csv": "src_sellers",
    "product_category_name_translation.csv": "src_category_translation"
}

def load_csv_to_postgres():
    print("🚀 Connecting to Docker PostgreSQL and moving data...")
    
    for csv_file, table_name in olist_tables.items():
        file_path = os.path.join(CSV_DIRECTORY, csv_file)
        
        if os.path.exists(file_path):
            print(f"Reading {csv_file} into memory...")
            df = pd.read_csv(file_path)
            
            print(f"Streaming rows into PostgreSQL table: {table_name}...")
            # 'if_exists=replace' ensures if you rerun this, it cleanly overwrites old data
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"✅ Successfully loaded {len(df)} rows.\n")
        else:
            print(f"⚠️ Missing file: {csv_file} not found at {file_path}. Skipping.")

    print("🎉 Step 1 Complete! All source data is securely inside your transactional database.")

if __name__ == "__main__":
    load_csv_to_postgres()