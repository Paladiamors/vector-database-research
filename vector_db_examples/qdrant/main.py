import json
import os
import sys

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import QdrantDB
from qdrant_client.http import models

def main():
    db = QdrantDB()

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
        print(f"ID: {res['id']}, Score: {res['score']:.4f}, Category: {res['payload']['category']}, Text: {res['payload']['text']}")

    # Metadata Search
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    filter_query = models.Filter(
        must=[
            models.FieldCondition(
                key="category",
                match=models.MatchValue(value="tech")
            )
        ]
    )
    scroll_result = db.client.scroll(
        collection_name=db.collection_name,
        scroll_filter=filter_query,
        limit=5,
        with_payload=True,
        with_vectors=False
    )
    for point in scroll_result[0]:
        print(f"ID: {point.id}, Category: {point.payload['category']}, Text: {point.payload['text']}")

    # Update
    print("\n--- Updating Metadata ---")
    point_id = 1
    db.client.set_payload(
        collection_name=db.collection_name,
        payload={"category": "food"},
        points=[point_id],
        wait=True
    )

    point = db.client.retrieve(db.collection_name, ids=[point_id])[0]
    print(f"After: Category = {point.payload['category']}")

    # Delete
    print("\n--- Deleting Point ---")
    db.delete_data(point_id)

    points = db.client.retrieve(db.collection_name, ids=[point_id])
    if not points:
        print("Point successfully deleted.")
    else:
        print("Point still exists.")

if __name__ == "__main__":
    main()
