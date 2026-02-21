import json
import os
import sys

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import MilvusDB
from pymilvus import Collection

def main():
    db = MilvusDB()

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
    print("\n--- Vector Search Results (Top 3 similar to item 0) ---")
    query_vector = data[0]["vector"]
    results = db.search(query_vector, limit=3)

    for res in results:
        print(f"ID: {res['id']}, Distance: {res['distance']:.4f}, Text: {res['text']}, Category: {res['category']}")

    # Metadata Search
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    collection = Collection(db.collection_name)
    expr = "category == 'tech'"
    results = collection.query(
        expr=expr,
        output_fields=["text", "category"]
    )
    for res in results:
        print(f"ID: {res['id']}, Text: {res['text']}, Category: {res['category']}")

    # Update (Upsert)
    print("\n--- Updating Metadata ---")
    item_to_update = data[0]

    upsert_data = [
        [item_to_update["id"]],
        [item_to_update["vector"]],
        ["food"],
        [item_to_update["text"]]
    ]
    collection.upsert(upsert_data)

    res = collection.query(expr=f"id == {item_to_update['id']}", output_fields=["category"])
    print(f"After Update: ID {res[0]['id']} Category = {res[0]['category']}")

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_to_update['id'])

    res = collection.query(expr=f"id == {item_to_update['id']}")
    if not res:
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

if __name__ == "__main__":
    main()
