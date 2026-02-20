from opensearchpy import OpenSearch
import json
import os

def main():
    # Connect
    print("Connecting to OpenSearch...")
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        http_compress=True,
        use_ssl=False
    )

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])
    index_name = "example_index"

    # 1. Create Index (Schema)
    if client.indices.exists(index_name):
        client.indices.delete(index_name)

    index_body = {
        "settings": {
            "index": {
                "knn": True
            }
        },
        "mappings": {
            "properties": {
                "vector": {
                    "type": "knn_vector",
                    "dimension": dim,
                    "method": {
                         "name": "hnsw",
                         "engine": "nmslib"
                    }
                },
                "text": {"type": "text"},
                "category": {"type": "keyword"}
            }
        }
    }

    client.indices.create(index=index_name, body=index_body)
    print(f"Index '{index_name}' created.")

    # 2. Index Documents
    print(f"Indexing {len(data)} documents...")
    for item in data:
        doc = {
            "vector": item["vector"],
            "text": item["text"],
            "category": item["metadata"]["category"]
        }
        client.index(index=index_name, id=str(item["id"]), body=doc)

    client.indices.refresh(index=index_name)
    print("Documents indexed.")

    # 3. Search (Vector Search via kNN)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]

    query = {
        "size": 3,
        "query": {
            "knn": {
                "vector": {
                    "vector": query_vector,
                    "k": 3
                }
            }
        }
    }

    response = client.search(index=index_name, body=query)

    for hit in response['hits']['hits']:
        print(f"ID: {hit['_id']}, Score: {hit['_score']:.4f}, Text: {hit['_source']['text']}, Category: {hit['_source']['category']}")

    # 4. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")

    # Filter using bool query
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

    response = client.search(index=index_name, body=query)

    for hit in response['hits']['hits']:
        print(f"ID: {hit['_id']}, Text: {hit['_source']['text']}, Category: {hit['_source']['category']}")

    # 5. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = str(data[0]["id"])

    # Verify before
    res = client.get(index=index_name, id=item_id)
    print(f"Before: {res['_source']['category']}")

    # Update
    client.update(index=index_name, id=item_id, body={"doc": {"category": "food"}})

    # Verify after
    res = client.get(index=index_name, id=item_id)
    print(f"After: {res['_source']['category']}")

    # 6. Delete Document
    print("\n--- Deleting Item ---")
    client.delete(index=index_name, id=item_id)

    # Verify
    if not client.exists(index=index_name, id=item_id):
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

if __name__ == "__main__":
    main()
