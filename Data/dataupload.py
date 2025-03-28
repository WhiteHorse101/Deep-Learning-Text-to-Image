import os
import requests
from pymongo import MongoClient
from azure.storage.blob import ContainerClient
from datetime import datetime

# ---------- CONFIGURATION ----------

# MongoDB
MONGO_URI = "mongodb+srv://saudagarrishabh:rishabh2001@deeplearning.gdgmz.mongodb.net/"
DATABASE_NAME = "unsplash_db"

# Collections to process (add or remove as needed)
COLLECTION_NAMES = [
    "chinese_architecture_images",
    "european_architecture_images",
    "indian_architecture_images",
    "japanese_architecture_images",
    "victorian_architecture_images"
]

# Azure Blob Container-Level SAS URL
# Example: "https://<account_name>.blob.core.windows.net/my-architecture-images?sv=...&ss=b&sp=rcwl..."
BLOB_CONTAINER_SAS_URL = "https://deeplearning02.blob.core.windows.net/architecture-images?sp=racwdl&st=2025-03-28T16:58:16Z&se=2025-04-23T23:58:16Z&spr=https&sv=2024-11-04&sr=c&sig=SdjPOg%2F2cOEDl3VthAwygIA5GHy49wX4nDW5yOkcenc%3D"

# Temporary folder to store downloaded images
TEMP_FOLDER = "temp_downloads"

# ----------------------------------


def download_image(image_url, image_id):
    """
    Downloads the image from 'image_url' and saves it locally as {image_id}.jpg.
    Returns the local file path if successful, else None.
    """
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    file_path = os.path.join(TEMP_FOLDER, f"{image_id}.jpg")
    
    try:
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()  # Raise an exception for bad HTTP status
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {image_url}: {e}")
        return None
    
    with open(file_path, "wb") as f:
        f.write(response.content)
    
    return file_path

def upload_to_azure_blob(file_path, container_client, category, image_id):
    """
    Uploads the local file at 'file_path' to Azure Blob Storage using a subfolder for the category.
    Returns the public blob URL (assuming your container SAS has read permissions).
    """
    # Construct a subfolder name from the category (fallback to "uncategorized" if None)
    if not category:
        category = "uncategorized"
    subfolder = category.lower().replace(" ", "_")
    
    blob_name = f"{subfolder}/{image_id}.jpg"
    
    print(f"Uploading {file_path} to blob '{blob_name}'...")
    
    blob_client = container_client.get_blob_client(blob=blob_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    # Construct the direct URL to the blob
    container_url = BLOB_CONTAINER_SAS_URL.split("?")[0]  # e.g., https://<account_name>.blob.core.windows.net/<container_name>
    blob_url = f"{container_url}/{blob_name}"
    
    return blob_url

def process_collection(collection_name, db, container_client):
    """
    Processes a single collection: downloads images, uploads to Azure Blob, updates MongoDB docs.
    """
    collection = db[collection_name]
    
    # Find docs without a 'blob_url' field (i.e., not uploaded yet)
    docs = collection.find({"blob_url": {"$exists": False}})
    
    count = 0
    for doc in docs:
        image_id = doc["image_id"]
        image_url = doc["image_url"]
        category = doc.get("category", collection_name)  # fallback to collection name if category missing
        
        print(f"\nCollection: {collection_name} | Processing image_id={image_id}, category={category}")
        
        # a) Download image
        local_path = download_image(image_url, image_id)
        if not local_path:
            print(f"Skipping image {image_id} due to download failure.")
            continue
        
        # b) Upload to Azure Blob (subfolder based on category)
        blob_url = upload_to_azure_blob(local_path, container_client, category, image_id)
        
        # c) Update the MongoDB document with the blob URL + timestamp
        collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "blob_url": blob_url,
                    "uploaded_at": datetime.utcnow()
                }
            }
        )
        
        # d) Remove local file
        os.remove(local_path)
        count += 1
        print(f"Done: {image_id} → {blob_url}")
    
    print(f"\nFinished processing {collection_name}. Uploaded {count} images.")


def main():
    # 1. Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    
    # 2. Create a ContainerClient from your SAS URL
    container_client = ContainerClient.from_container_url(BLOB_CONTAINER_SAS_URL)
    
    # 3. Iterate over each collection in COLLECTION_NAMES
    for coll_name in COLLECTION_NAMES:
        print(f"\n=== Starting collection: {coll_name} ===")
        process_collection(coll_name, db, container_client)

if __name__ == "__main__":
    main()
