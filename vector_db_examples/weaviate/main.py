import weaviate
import json
import os
import time

def main():
    # Connect to Weaviate
    client = weaviate.connect_to_local(
        port=8080,
        grpc_port=50051
    )

    try:
        # Check if connected
        if not client.is_ready():
            print("Weaviate is not ready.")
            return

        print("Connected to Weaviate.")

        # Load dataset
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_path = os.path.join(script_dir, '../data/dataset.json')
        with open(data_path, 'r') as f:
            dataset = json.load(f)

        # 1. Create Collection (Schema)
        collection_name = "Document"
        # Delete if exists
        if client.collections.exists(collection_name):
            client.collections.delete(collection_name)

        # Create collection
        # We define properties explicitly for better control, though auto-schema works too
        client.collections.create(
            name=collection_name,
            properties=[
                weaviate.classes.config.Property(name="text", data_type=weaviate.classes.config.DataType.TEXT),
                weaviate.classes.config.Property(name="category", data_type=weaviate.classes.config.DataType.TEXT),
            ],
            vectorizer_config=weaviate.classes.config.Configure.Vectorizer.none(), # We provide vectors manually
        )
        print(f"Created collection '{collection_name}'.")

        collection = client.collections.get(collection_name)

        # 2. Store Embeddings (Batch Insert)
        with collection.batch.dynamic() as batch:
            for item in dataset:
                batch.add_object(
                    properties={
                        "text": item["text"],
                        "category": item["metadata"]["category"]
                    },
                    vector=item["vector"],
                    uuid=weaviate.util.generate_uuid5(item["id"]) # Deterministic UUID based on ID
                )

        if len(collection.batch.failed_objects) > 0:
            print("Some objects failed to insert.")
            for failed in collection.batch.failed_objects:
                print(failed)
        else:
            print(f"Inserted {len(dataset)} objects.")

        # 3. Perform Search (Vector Search)
        # Using the vector of the first item as a query vector to find similar items
        query_vector = dataset[0]["vector"]

        response = collection.query.near_vector(
            near_vector=query_vector,
            limit=3,
            return_metadata=weaviate.classes.query.MetadataQuery(distance=True)
        )

        print("\n--- Vector Search Results (Top 3 similar to 'Apple is a popular fruit.') ---")
        for o in response.objects:
            print(f"Text: {o.properties['text']}, Category: {o.properties['category']}, Distance: {o.metadata.distance:.4f}")

        # 4. Search by Metadata (Filtering)
        # Filter for category 'tech'
        response = collection.query.fetch_objects(
            filters=weaviate.classes.query.Filter.by_property("category").equal("tech"),
            limit=5
        )

        print("\n--- Metadata Search Results (Category: tech) ---")
        for o in response.objects:
            print(f"Text: {o.properties['text']}, Category: {o.properties['category']}")

        # 5. Update Metadata
        # Update the category of the first item (Apple) to 'food'
        item_uuid = weaviate.util.generate_uuid5(dataset[0]["id"])

        # Fetch object to verify before update
        obj = collection.query.fetch_object_by_id(item_uuid)
        print(f"\nBefore Update: Category = {obj.properties['category']}")

        collection.data.update(
            uuid=item_uuid,
            properties={"category": "food"}
        )

        obj = collection.query.fetch_object_by_id(item_uuid)
        print(f"After Update: Category = {obj.properties['category']}")

        # 6. Delete Embedding
        # Delete the item we just updated
        collection.data.delete_by_id(item_uuid)
        print(f"\nDeleted object with UUID: {item_uuid}")

        # Verify deletion
        obj = collection.query.fetch_object_by_id(item_uuid)
        if obj is None:
            print("Object successfully deleted.")
        else:
            print("Object still exists.")

    finally:
        client.close()

if __name__ == "__main__":
    main()
