import chromadb
from chromadb.config import Settings
import json
import os

def main():
    # 1. Connect
    print("Connecting to Chroma...")
    # client = chromadb.PersistentClient(path="./chroma_db") # For local
    # For Docker server
    client = chromadb.HttpClient(host='localhost', port=8000)

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        dataset = json.load(f)

    collection_name = "example_collection"

    # 2. Create Collection
    # Delete if exists
    try:
        client.delete_collection(collection_name)
    except:
        pass

    collection = client.create_collection(name=collection_name)
    print(f"Created collection '{collection_name}'.")

    # 3. Add Data
    ids = [str(item["id"]) for item in dataset] # IDs must be strings
    embeddings = [item["vector"] for item in dataset]
    metadatas = [{"category": item["metadata"]["category"]} for item in dataset]
    documents = [item["text"] for item in dataset]

    collection.add(
        ids=ids,
        embeddings=embeddings,
        metadatas=metadatas,
        documents=documents
    )
    print(f"Added {len(ids)} items.")

    # 4. Search (Vector Search)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = dataset[0]["vector"]

    results = collection.query(
        query_embeddings=[query_vector],
        n_results=3
    )

    for i, id in enumerate(results['ids'][0]):
        print(f"ID: {id}, Distance: {results['distances'][0][i]:.4f}, Text: {results['documents'][0][i]}, Metadata: {results['metadatas'][0][i]}")

    # 5. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    results = collection.query(
        query_embeddings=[query_vector], # We need a query embedding usually, or use get()
        n_results=5,
        where={"category": "tech"}
    )
    # Alternatively use get() for pure filtering
    # results = collection.get(where={"category": "tech"})

    for i, id in enumerate(results['ids'][0]):
         print(f"ID: {id}, Text: {results['documents'][0][i]}, Metadata: {results['metadatas'][0][i]}")

    # 6. Update Metadata
    # Update item 1 (Apple) to category 'food'
    print("\n--- Updating Metadata ---")
    item_id = str(dataset[0]["id"])

    # Verify before
    item = collection.get(ids=[item_id])
    print(f"Before: Metadata = {item['metadatas'][0]}")

    collection.update(
        ids=[item_id],
        metadatas=[{"category": "food"}]
    )

    # Verify after
    item = collection.get(ids=[item_id])
    print(f"After: Metadata = {item['metadatas'][0]}")

    # 7. Delete Item
    print("\n--- Deleting Item ---")
    collection.delete(ids=[item_id])

    # Verify deletion
    item = collection.get(ids=[item_id])
    if not item['ids']:
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

if __name__ == "__main__":
    main()
