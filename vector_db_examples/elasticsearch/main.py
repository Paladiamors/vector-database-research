from elasticsearch import Elasticsearch
import json
import os
import time

def main():
    # Connect
    print("Connecting to Elasticsearch...")
    es = Elasticsearch("http://localhost:9200")

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])
    index_name = "example_index"

    # 1. Create Index (Schema)
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    mapping = {
        "properties": {
            "text": {"type": "text"},
            "category": {"type": "keyword"},
            "vector": {
                "type": "dense_vector",
                "dims": dim,
                "index": True,
                "similarity": "cosine"
            }
        }
    }

    es.indices.create(index=index_name, mappings=mapping)
    print(f"Index '{index_name}' created.")

    # 2. Index Documents
    print(f"Indexing {len(data)} documents...")
    actions = []
    for item in data:
        doc = {
            "text": item["text"],
            "category": item["metadata"]["category"],
            "vector": item["vector"]
        }
        es.index(index=index_name, id=str(item["id"]), document=doc)

    # Refresh to make searchable immediately
    es.indices.refresh(index=index_name)
    print("Documents indexed.")

    # 3. Search (Vector Search via kNN)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]

    # In ES 8.x, knn search is top-level parameter
    response = es.search(
        index=index_name,
        knn={
            "field": "vector",
            "query_vector": query_vector,
            "k": 3,
            "num_candidates": 100
        },
        source=["text", "category"]
    )

    for hit in response['hits']['hits']:
        print(f"ID: {hit['_id']}, Score: {hit['_score']:.4f}, Text: {hit['_source']['text']}, Category: {hit['_source']['category']}")

    # 4. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")

    # Combine kNN with filter
    # Or just use filter query without vector search?
    # Prompt asks "search by meta data".
    # Standard filter query
    response = es.search(
        index=index_name,
        query={
            "term": {
                "category": "tech"
            }
        },
        source=["text", "category"]
    )

    for hit in response['hits']['hits']:
        print(f"ID: {hit['_id']}, Text: {hit['_source']['text']}, Category: {hit['_source']['category']}")

    # 5. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = str(data[0]["id"])

    # Verify before
    res = es.get(index=index_name, id=item_id)
    print(f"Before: {res['_source']['category']}")

    # Update
    es.update(index=index_name, id=item_id, doc={"category": "food"})

    # Verify after
    res = es.get(index=index_name, id=item_id)
    print(f"After: {res['_source']['category']}")

    # 6. Delete Document
    print("\n--- Deleting Item ---")
    es.delete(index=index_name, id=item_id)

    # Verify
    if not es.exists(index=index_name, id=item_id):
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

if __name__ == "__main__":
    main()
