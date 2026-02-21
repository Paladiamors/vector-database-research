import json
import os
import sys

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import OpenSearchDB

def main():
    db = OpenSearchDB()

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
    query = {
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "category": "tech"
                    }
                }
            }
        }
    }
    response = db.client.search(index=db.index_name, body=query)
    for hit in response['hits']['hits']:
        print(f"ID: {hit['_id']}, Text: {hit['_source']['text']}, Category: {hit['_source']['category']}")

    # Update
    print("\n--- Updating Metadata ---")
    item_id = str(data[0]["id"])
    db.client.update(index=db.index_name, id=item_id, body={"doc": {"category": "food"}})

    res = db.client.get(index=db.index_name, id=item_id)
    print(f"After: {res['_source']['category']}")

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_id)

    if not db.client.exists(index=db.index_name, id=item_id):
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

if __name__ == "__main__":
    main()
