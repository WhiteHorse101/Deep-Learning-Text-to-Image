import requests
from pymongo import MongoClient
from datetime import datetime, timedelta
import time

# Replace with your Unsplash API Access Key
ACCESS_KEY = 't_pLyTJ45k2OsWx-yIauk1YPoUVWbcpkZYRRG_qAOUY'
UNSPLASH_URL = "https://api.unsplash.com/search/photos"

# MongoDB configuration
MONGO_URI = "mongodb+srv://saudagarrishabh:rishabh2001@deeplearning.gdgmz.mongodb.net/"
DATABASE_NAME = "unsplash_db"

# Unsplash API limits and request settings
MAX_REQUESTS_PER_HOUR = 50  # Unsplash API limit per hour
PER_PAGE = 30               # Maximum images per page returned by Unsplash
TARGET_IMAGES_PER_CATEGORY = 1500

# Global counter and start time for the current hour window
REQUESTS_MADE = 0
hour_start_time = datetime.now()

def check_rate_limit():
    global REQUESTS_MADE, hour_start_time
    if REQUESTS_MADE >= MAX_REQUESTS_PER_HOUR:
        # Calculate remaining time in the hour window
        elapsed = datetime.now() - hour_start_time
        if elapsed < timedelta(hours=1):
            wait_seconds = (timedelta(hours=1) - elapsed).total_seconds()
            print(f"Reached API limit. Sleeping for {int(wait_seconds)} seconds...")
            time.sleep(wait_seconds)
        # Reset the counter and start time after waiting
        REQUESTS_MADE = 0
        hour_start_time = datetime.now()

def search_unsplash(query, per_page=PER_PAGE, page=1):
    global REQUESTS_MADE
    params = {
        "query": query,
        "per_page": per_page,
        "page": page,
        "client_id": ACCESS_KEY
    }
    response = requests.get(UNSPLASH_URL, params=params)
    REQUESTS_MADE += 1  # Increment global request counter

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def store_data_mongodb(data, collection, category):
    """Store Unsplash search results in MongoDB and return the count of newly inserted images."""
    if not data or "results" not in data:
        return 0

    inserted_count = 0
    for photo in data["results"]:
        document = {
            "image_id": photo["id"],
            "image_url": photo["urls"]["regular"],
            "text_description": photo.get("alt_description", "No description provided"),
            "category": category,
            "retrieved_at": datetime.utcnow(),
            "raw_data": photo
        }
        # Upsert the document to avoid duplicates
        result = collection.update_one(
            {"image_id": photo["id"]},
            {"$setOnInsert": document},
            upsert=True
        )
        # If the document is newly inserted, matched_count will be 0
        if result.matched_count == 0:
            inserted_count += 1
    return inserted_count

def main():
    global REQUESTS_MADE, hour_start_time

    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]

    # Define categories without "Indian Architecture"
    categories = [
       "European Architecture",
       "Japanese Architecture",
       "Chinese Architecture"
    ]

    for category in categories:
        print(f"\n=== Processing category: {category} ===")
        # Create or reference the collection for the category
        collection_name = category.lower().replace(" ", "_") + "_images"
        collection = db[collection_name]
        # Create unique index on image_id to avoid duplicates
        collection.create_index("image_id", unique=True)

        collected_count = 0
        page = 1
        while collected_count < TARGET_IMAGES_PER_CATEGORY:
            check_rate_limit()  # Check if we've hit the hourly limit and wait if needed

            print(f"Query: {category}, Page: {page}")
            data = search_unsplash(category, per_page=PER_PAGE, page=page)
            if data is None or "results" not in data:
                print(f"No data returned for {category} on page {page}. Moving to next category.")
                break

            inserted = store_data_mongodb(data, collection, category)
            collected_count += inserted
            print(f"Inserted: {inserted} new images, Total collected for {category}: {collected_count}/{TARGET_IMAGES_PER_CATEGORY}")

            # If the API returns fewer images than PER_PAGE, assume there are no more results for this category.
            if len(data["results"]) < PER_PAGE:
                print(f"Less than {PER_PAGE} images returned. Likely no more images for {category} at page {page}.")
                break

            page += 1

        print(f"Finished processing category: {category}. Total images collected: {collected_count}")

if __name__ == "__main__":
    main()
