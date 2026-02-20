import redis
import numpy as np
import json
import os
from redis.commands.search.field import TextField, TagField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

def main():
    # Connect
    print("Connecting to Redis...")
    r = redis.Redis(host='localhost', port=6379, decode_responses=False)

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])

    index_name = "idx:items"
    prefix = "item:"

    # 1. Create Index (Schema)
    try:
        r.ft(index_name).info()
        print("Index already exists, dropping it...")
        r.ft(index_name).dropindex()
    except:
        pass

    # Define schema
    # Note: VectorField arguments depend on algorithm (FLAT or HNSW)
    schema = (
        TextField("text"),
        TagField("category"),
        VectorField("vector",
            "FLAT", {
                "TYPE": "FLOAT32",
                "DIM": dim,
                "DISTANCE_METRIC": "COSINE"
            }
        )
    )

    definition = IndexDefinition(prefix=[prefix], index_type=IndexType.HASH)

    r.ft(index_name).create_index(schema, definition=definition)
    print(f"Index '{index_name}' created.")

    # 2. Add Data
    print(f"Adding {len(data)} items...")
    pipeline = r.pipeline()
    for item in data:
        key = f"{prefix}{item['id']}"
        vector = np.array(item["vector"], dtype=np.float32).tobytes()

        pipeline.hset(key, mapping={
            "text": item["text"],
            "category": item["metadata"]["category"],
            "vector": vector
        })
    pipeline.execute()
    print("Data added.")

    # 3. Search (Vector Search)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = np.array(data[0]["vector"], dtype=np.float32).tobytes()

    # Query: "*=>[KNN 3 @vector $vec AS score]"
    q = Query("*=>[KNN 3 @vector $vec AS score]")\
        .sort_by("score")\
        .return_fields("score", "text", "category")\
        .dialect(2)

    params = {"vec": query_vector}

    res = r.ft(index_name).search(q, query_params=params)

    for doc in res.docs:
        print(f"ID: {doc.id}, Score: {doc.score}, Text: {doc.text}, Category: {doc.category}")

    # 4. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    # Query: "@category:{tech}"
    q = Query("@category:{tech}").return_fields("text", "category")

    res = r.ft(index_name).search(q)

    for doc in res.docs:
        print(f"ID: {doc.id}, Text: {doc.text}, Category: {doc.category}")

    # 5. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]
    key = f"{prefix}{item_id}"

    # Verify before
    print(f"Before: {r.hget(key, 'category').decode('utf-8')}")

    # Update field
    r.hset(key, "category", "food")

    # Verify after
    print(f"After: {r.hget(key, 'category').decode('utf-8')}")

    # 6. Delete Item
    print("\n--- Deleting Item ---")
    r.delete(key)

    # Verify
    if not r.exists(key):
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

if __name__ == "__main__":
    main()
