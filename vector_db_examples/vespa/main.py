import json
import os
import sys

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import VespaDB

def main():
    db = VespaDB()

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
        print(f"ID: {res['id']}, Score: {res['score']:.4f}, Text: {res['text']}, Category: {res['category']}")

    # Metadata Search
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    res = db.app.query(
        yql="select * from sources * where category contains 'tech'"
    )
    if res.hits:
        for hit in res.hits:
            print(f"ID: {hit['id']}, Text: {hit['fields']['text']}, Category: {hit['fields']['category']}")

    # Update
    print("\n--- Updating Metadata ---")
    item_id = str(data[0]["id"])
    db.app.update_data(
        schema="doc",
        data_id=item_id,
        fields={"category": "food"}
    )

    res = db.app.get_data(schema="doc", data_id=item_id)
    if res.json:
        print(f"After: {res.json['fields']['category']}")

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_id)
    print("Item deleted.")

if __name__ == "__main__":
    main()
