import os
import json
import pandas as pd
from pymongo import MongoClient
from prefect import task, flow

@task
def get_mongo_connection():
    try:
        connection_string = os.getenv('MONGODB_CONNECTION_STRING')
        client = MongoClient(connection_string)
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

@task
def extract_data():
    try:
        client = get_mongo_connection()
        db = client[os.getenv('DB_NAME')]
        collection = db[os.getenv('COLLECTION_NAME')]

        # Fetch all sales data
        sales_data = collection.find({})

        df = pd.DataFrame(list(sales_data))
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        client.close()
        return df
    
    except Exception as e:
        print(f"An error occurred during extraction: {e}")
        return pd.DataFrame()

@task
def calculate_spending_per_receipt(df):
    # Calculate Spending_per_receipt
    total_sales = df['line_item_amount'].sum()
    total_receipts = df['transaction_id'].nunique()
    spending_per_receipt = total_sales / total_receipts if total_receipts else 0
    return spending_per_receipt

@task
def calculate_items_per_receipt(df):
    # Calculate Items_per_receipt
    total_receipts = df['transaction_id'].nunique()
    total_items = df['line_item_id'].count()
    items_per_receipt = total_items / total_receipts if total_receipts else 0
    return items_per_receipt

@task
def calculate_sales_comparison(df, start_date_st, start_date_nd, comparison_type='daily'):
    try:
        # Check if 'line_item_amount' exist in df
        if 'line_item_amount' not in df.columns:
            df['line_item_amount'] = df['quantity'] * df['unit_price']
        
        df.set_index('transaction_date', inplace=True)

        if comparison_type == 'daily':
            end_date_1 = start_date_st + pd.DateOffset(days=1)
            end_date_2 = start_date_nd + pd.DateOffset(days=1)
        elif comparison_type == 'weekly':
            end_date_1 = start_date_st + pd.DateOffset(days=7)
            end_date_2 = start_date_nd + pd.DateOffset(days=7)
        elif comparison_type == 'monthly':
            end_date_1 = start_date_st + pd.DateOffset(months=1)
            end_date_2 = start_date_nd + pd.DateOffset(months=1)
        else:
            raise ValueError("Comparison type must be one of: daily, weekly, monthly")

        # Filter data for the specified date ranges
        sales_range_1 = df[(df.index >= start_date_st) & (df.index < end_date_1)]
        sales_range_2 = df[(df.index >= start_date_nd) & (df.index < end_date_2)]

        sales_range_1_total = sales_range_1['line_item_amount'].sum()
        sales_range_2_total = sales_range_2['line_item_amount'].sum()

        sales_comparison = {
            'sales_range_1_total': float(sales_range_1_total),
            'sales_range_2_total': float(sales_range_2_total),
            'sales_difference': float(sales_range_2_total - sales_range_1_total)
        }

        return sales_comparison

    except Exception as e:
        print(f"An error occurred during sales comparison: {e}")
        return {}

@task
def transform_data(df, start_date_st, start_date_nd, comparison_type='daily'):
    try:
        spending_per_receipt = calculate_spending_per_receipt(df)
        items_per_receipt = calculate_items_per_receipt(df)
        sales_comparison = calculate_sales_comparison(df, start_date_st, start_date_nd, comparison_type)

        metrics = {
            'spending_per_receipt': float(spending_per_receipt),
            'items_per_receipt': float(items_per_receipt),
            'sales_comparison': sales_comparison
        }

        return metrics
    except Exception as e:
        print(f"An error occurred during transformation: {e}")
        return {}
    
@task
def load_data(metrics):
    try:
        client = get_mongo_connection()
        db = client[os.getenv('DB_NAME')]
        collection = db['sales_metrics']
        collection.insert_one(metrics)
        client.close()
    except Exception as e:
        print(f"An error occurred while loading data: {e}")

@task
def save_to_json(metrics, file_path='metrics.json'):
    # Convert ObjectIds to strings
    if '_id' in metrics:
        metrics['_id'] = str(metrics['_id'])

    with open(file_path, 'w') as json_file:
        json.dump(metrics, json_file, indent=4)

@flow
def etl_flow(start_date_st, start_date_nd, comparison_type='daily'):
    data = extract_data()
    metrics = transform_data(data, start_date_st, start_date_nd, comparison_type)
    if metrics:
        load_data(metrics)
        save_to_json(metrics)
