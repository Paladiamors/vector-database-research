import json
import os
import sys
import numpy as np

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import RedisDB
from redis.commands.search.query import Query

def main():
    db = RedisDB()

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])

    # Setup
    db.setup(dim)

    # Insert
    db.insert_data(data)

    # Search
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]
    results = db.search(query_vector, limit=3)

    for res in results:
        print(f"ID: {res['id']}, Score: {res['score']}, Text: {res['text']}, Category: {res['category']}")

    # Metadata Search
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    q = Query("@category:{tech}").return_fields("text", "category")
    res = db.client.ft(db.index_name).search(q)
    for doc in res.docs:
        print(f"ID: {doc.id}, Text: {doc.text}, Category: {doc.category}")

    # Update
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]
    key = f"{db.prefix}{item_id}"

    print(f"Before: {db.client.hget(key, 'category').decode('utf-8')}")
    db.client.hset(key, "category", "food")
    print(f"After: {db.client.hget(key, 'category').decode('utf-8')}")

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_id)

    if not db.client.exists(key):
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

if __name__ == "__main__":
    main()
