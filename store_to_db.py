import os
import json
import pandas as pd
from pymongo import MongoClient

def load_config(config_path):
    """
    Load configuration from a JSON file.
    """
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def remove_columns(df, columns_to_exclude):
    """
    Remove specified columns from a DataFrame.

    Args:
        df : The input DataFrame.
        columns_to_exclude : List of columns to exclude from the DataFrame.

    Returns:
        pd.DataFrame: DataFrame without the specified columns.
    """
    return df.drop(columns=columns_to_exclude, errors='ignore')

def process_and_store_csv(data_path, columns_to_drop, db, collection_name):
    """
    Load a CSV file, remove specified columns, and store the result in MongoDB.

    Args:
    data_path : Path to the CSV file.
    columns_to_drop : List of columns to remove.
    db : MongoDB client instance.
    collection_name : Name of the MongoDB collection to store data.
    """
    df = pd.read_csv(data_path)
    modified_df = remove_columns(df, columns_to_drop)
    collection = db[collection_name]
    try:
        collection.insert_many(modified_df.to_dict('records'))
        print(f"Data from {data_path} has been successfully stored into MongoDB.")
    except Exception as e:
        print(f"An error occurred while inserting data from {data_path}: {e}")

def store_data_to_mongodb(config_path):
    """
    Modify, save, and store CSV data into MongoDB.
    """
    try:
        # Load configuration
        config = load_config(config_path)
        data_files = config['data_files']

        # Connect to MongoDB
        connection_string = os.getenv('MONGODB_CONNECTION_STRING')
        client = MongoClient(connection_string)
        db = client[os.getenv('DB_NAME')]

        # Process and store data
        for file_config in data_files:
            data_path = file_config['path']
            columns_to_drop = file_config['columns_to_drop']
            if os.path.exists(data_path):
                collection_name = os.path.basename(data_path).split('.')[0].replace(' ', '_')
                process_and_store_csv(data_path, columns_to_drop, db, collection_name)
            else:
                print(f"File {data_path} does not exist and was skipped.")

        print("All data has been successfully stored into MongoDB.")
    except Exception as e:
        print(f"An error occurred: {e}")
