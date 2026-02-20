from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)
import json
import numpy as np
import os

def main():
    # 1. Connect to Milvus
    print("Connecting to Milvus...")
    connections.connect("default", host="localhost", port="19530")

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        dataset = json.load(f)

    collection_name = "example_collection"

    # 2. Define Schema
    # Check if collection exists
    if utility.has_collection(collection_name):
        utility.drop_collection(collection_name)

    dim = len(dataset[0]["vector"])

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
        FieldSchema(name="category", dtype=DataType.VARCHAR, max_length=100),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=1000)
    ]

    schema = CollectionSchema(fields, "Example collection for vector search")
    collection = Collection(collection_name, schema)

    # 3. Insert Data
    # Prepare data in column format
    ids = [item["id"] for item in dataset]
    vectors = [item["vector"] for item in dataset]
    categories = [item["metadata"]["category"] for item in dataset]
    texts = [item["text"] for item in dataset]

    entities = [
        ids,
        vectors,
        categories,
        texts
    ]

    insert_result = collection.insert(entities)
    print(f"Inserted {insert_result.num_rows} entities.")

    # 4. Create Index
    index_params = {
        "metric_type": "L2",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 1024}
    }
    collection.create_index(field_name="vector", index_params=index_params)
    collection.load()

    # 5. Search (Vector Search)
    print("\n--- Vector Search Results (Top 3 similar to item 0) ---")
    search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
    query_vector = [dataset[0]["vector"]]

    results = collection.search(
        data=query_vector,
        anns_field="vector",
        param=search_params,
        limit=3,
        output_fields=["text", "category"]
    )

    for hits in results:
        for hit in hits:
            print(f"ID: {hit.id}, Distance: {hit.distance}, Text: {hit.entity.get('text')}, Category: {hit.entity.get('category')}")

    # 6. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    expr = "category == 'tech'"
    results = collection.query(
        expr=expr,
        output_fields=["text", "category"]
    )
    for res in results:
        print(f"ID: {res['id']}, Text: {res['text']}, Category: {res['category']}")

    # 7. Update Metadata (Upsert)
    # We update item 1 (Apple) to have category 'food'
    # Upsert requires providing all fields for the row
    print("\n--- Updating Metadata ---")
    item_to_update = dataset[0]

    upsert_data = [
        [item_to_update["id"]],
        [item_to_update["vector"]],
        ["food"], # New category
        [item_to_update["text"]]
    ]

    collection.upsert(upsert_data)

    # Verify update
    res = collection.query(expr=f"id == {item_to_update['id']}", output_fields=["category"])
    print(f"After Update: ID {res[0]['id']} Category = {res[0]['category']}")

    # 8. Delete
    print("\n--- Deleting Item ---")
    expr = f"id == {item_to_update['id']}"
    collection.delete(expr)

    # Verify deletion
    res = collection.query(expr=f"id == {item_to_update['id']}")
    if not res:
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

    # Cleanup
    # collection.drop()

if __name__ == "__main__":
    main()
