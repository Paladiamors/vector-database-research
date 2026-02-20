from qdrant_client import QdrantClient
from qdrant_client.http import models
import json
import os

def main():
    # 1. Connect
    print("Connecting to Qdrant...")
    client = QdrantClient(host="localhost", port=6333)

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        dataset = json.load(f)

    collection_name = "example_collection"

    # 2. Create Collection
    # Recreate collection if exists
    if client.collection_exists(collection_name):
        client.delete_collection(collection_name)

    dim = len(dataset[0]["vector"])

    client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE)
    )
    print(f"Created collection '{collection_name}' with dimension {dim}.")

    # 3. Insert Points
    points = []
    for item in dataset:
        points.append(models.PointStruct(
            id=item["id"],
            vector=item["vector"],
            payload={
                "text": item["text"],
                "category": item["metadata"]["category"]
            }
        ))

    operation_info = client.upsert(
        collection_name=collection_name,
        wait=True,
        points=points
    )
    print(f"Upsert status: {operation_info.status}")

    # 4. Search (Vector Search)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = dataset[0]["vector"]

    search_result = client.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=3
    )

    for hit in search_result:
        print(f"ID: {hit.id}, Score: {hit.score:.4f}, Category: {hit.payload['category']}, Text: {hit.payload['text']}")

    # 5. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")

    # Using Filter
    filter_query = models.Filter(
        must=[
            models.FieldCondition(
                key="category",
                match=models.MatchValue(value="tech")
            )
        ]
    )

    # Use scroll to list points matching filter (since search requires vector)
    scroll_result = client.scroll(
        collection_name=collection_name,
        scroll_filter=filter_query,
        limit=5,
        with_payload=True,
        with_vectors=False
    )

    for point in scroll_result[0]:
        print(f"ID: {point.id}, Category: {point.payload['category']}, Text: {point.payload['text']}")

    # 6. Update Metadata
    # Update payload for item 1
    print("\n--- Updating Metadata ---")
    point_id = 1

    # Verify before
    point = client.retrieve(collection_name, ids=[point_id])[0]
    print(f"Before: Category = {point.payload['category']}")

    client.set_payload(
        collection_name=collection_name,
        payload={"category": "food"},
        points=[point_id],
        wait=True
    )

    # Verify after
    point = client.retrieve(collection_name, ids=[point_id])[0]
    print(f"After: Category = {point.payload['category']}")

    # 7. Delete Point
    print("\n--- Deleting Point ---")
    client.delete(
        collection_name=collection_name,
        points_selector=models.PointIdsList(points=[point_id]),
        wait=True
    )

    # Verify deletion
    points = client.retrieve(collection_name, ids=[point_id])
    if not points:
        print("Point successfully deleted.")
    else:
        print("Point still exists.")

if __name__ == "__main__":
    main()
