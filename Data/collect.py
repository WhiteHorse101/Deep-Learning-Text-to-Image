import requests
from pymongo import MongoClient
from datetime import datetime

# Replace with your Unsplash API Access Key
ACCESS_KEY = 't_pLyTJ45k2OsWx-yIauk1YPoUVWbcpkZYRRG_qAOUY'
UNSPLASH_URL = "https://api.unsplash.com/search/photos"

# MongoDB configuration: Replace with your connection details if not using a local instance
MONGO_URI = "mongodb+srv://saudagarrishabh:rishabh2001@deeplearning.gdgmz.mongodb.net/"
DATABASE_NAME = "unsplash_db"
COLLECTION_NAME = "images"

def search_unsplash(query, per_page=10, page=1):
    params = {
        "query": query,
        "per_page": per_page,
        "page": page,
        "client_id": ACCESS_KEY
    }
    response = requests.get(UNSPLASH_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code, response.text)
        return None

def store_data_mongodb(data, collection):
    if data and "results" in data:
        for photo in data["results"]:
            document = {
                "image_id": photo["id"],
                "image_url": photo["urls"]["regular"],
                "text_description": photo.get("alt_description", "No description provided"),
                "category": "unspecified",  # Modify based on query or classification logic
                "retrieved_at": datetime.utcnow(),
                "raw_data": photo  # Store the complete metadata if needed
            }
            # Insert document if not already present, otherwise update
            collection.update_one(
                {"image_id": photo["id"]},
                {"$set": document},
                upsert=True
            )
        print(f"Stored {len(data['results'])} documents in MongoDB.")

def main():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    
    # Create a unique index on image_id to avoid duplicates
    collection.create_index("image_id", unique=True)
    
    # Define your search queries (you can add more as needed)
    queries = ["Indian Architecture", "Victorian Architecture", "European Architecture"]
    
    for query in queries:
        print("Searching for:", query)
        # You can loop through multiple pages if needed
        data = search_unsplash(query, per_page=10, page=1)
        store_data_mongodb(data, collection)
    
    print("Data retrieval and storage complete.")

if __name__ == "__main__":
    main()
