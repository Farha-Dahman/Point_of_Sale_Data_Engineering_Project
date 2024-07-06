import os
from pymongo import MongoClient
from prefect import task

@task
def get_mongo_connection():
    try:
        connection_string = os.getenv('MONGODB_CONNECTION_STRING')
        client = MongoClient(connection_string)
        db = client[os.getenv('DB_NAME')]
        sales_collection = db[os.getenv('COLLECTION_NAME')]
        return client, db, sales_collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None, None, None
