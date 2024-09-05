import os
import json
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

load_dotenv()

# Azure Blob Storage configuration
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
USERNAME = os.getenv("MONGO_USERNAME")
PASSWORD = os.getenv("MONGO_PASSWORD")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME")
CLUSTER_ADDRESS = os.getenv("MONGO_CLUSTER_ADDRESS")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COUNT = int(os.getenv("COUNT"))

MONGODB_URI = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER_ADDRESS}/?retryWrites=true&w=majority&appName={CLUSTER_NAME}"

# Initialize Azure Blob Storage client
blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Ensure the container exists
try:
    container_client.create_container()
except ResourceExistsError:
    pass

def upload_to_blob_storage(file_path, blob_name):
    """Upload a file to Azure Blob Storage"""
    blob_client = container_client.get_blob_client(blob_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded {file_path} to blob storage as {blob_name}")

def manage_backups():
    """Keep only the most recent 7 backups based on folder names"""
    blobs = container_client.list_blobs()
    
    # Collect folder names and sort them
    folder_names = set()
    for blob in blobs:
        folder_name = os.path.dirname(blob.name)
        folder_names.add(folder_name)
    
    # Sort folder names by timestamp (assuming folder names are in timestamp format)
    sorted_folders = sorted(folder_names, reverse=True)

    # Keep only the most recent 7 backups
    if len(sorted_folders) > COUNT:
        folders_to_delete = sorted_folders[COUNT:]
        for folder in folders_to_delete:
            # Delete all blobs in the folder
            blobs_to_delete = container_client.list_blobs(name_starts_with=folder)
            for blob in blobs_to_delete:
                blob_client = container_client.get_blob_client(blob.name)
                blob_client.delete_blob()
                print(f"Deleted old backup: {blob.name}")

def mongo_to_dict(data):
    """Convert MongoDB data to a serializable dictionary"""
    if isinstance(data, ObjectId):
        return {"$oid": str(data)}
    elif isinstance(data, datetime):
        return {"$date": data}
    elif isinstance(data, dict):
        return {key: mongo_to_dict(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [mongo_to_dict(item) for item in data]
    else:
        return data

client = MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]

# Get all collection names
collections = db.list_collection_names()

# Prepare timestamped directory name
timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
timestamp_dir = f"backup/{timestamp}"
os.makedirs(timestamp_dir, exist_ok=True)

for collection_name in collections:
    collection = db[collection_name]

    # Fetch data from MongoDB collection
    cursor = collection.find({})

    # Prepare JSON file path
    json_file_path = os.path.join(timestamp_dir, f"{collection_name}.json")

    # Write data to JSON file
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        data = [mongo_to_dict(doc) for doc in cursor]
        json.dump(data, json_file, indent=4, default=str)  # Convert to JSON and handle non-serializable objects like ObjectId

    # Upload JSON to Azure Blob Storage
    blob_name = f"{timestamp}/{collection_name}.json"
    upload_to_blob_storage(json_file_path, blob_name)

    os.remove(json_file_path)
os.removedirs(timestamp_dir)

# Manage backups: keep only the most recent 7 backups
manage_backups()
