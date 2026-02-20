import faiss
import numpy as np
import json
import pickle
import os

class FaissStore:
    def __init__(self, dim, index_path="faiss_index.index", metadata_path="faiss_meta.pkl"):
        self.dim = dim
        self.index_path = index_path
        self.metadata_path = metadata_path

        # Initialize Index
        # IndexFlatL2 does not support add_with_ids directly, so we wrap it in IndexIDMap
        self.index = faiss.IndexIDMap(faiss.IndexFlatL2(dim))

        # Metadata storage: Map ID (int) -> Metadata (dict)
        # Faiss uses integer IDs (0 to N-1 for add, or explicit for add_with_ids)
        # We will use add_with_ids to map our external IDs (if int) or map internal ID to external ID
        self.id_map = {} # Internal ID -> External ID
        self.metadata_store = {} # External ID -> {text, metadata, vector}
        self.next_id = 0

    def add(self, ids, vectors, texts, metadatas):
        vectors = np.array(vectors).astype('float32')
        ids = np.array(ids).astype('int64')

        # Add to index
        self.index.add_with_ids(vectors, ids)

        # Add to metadata store
        for i, id_val in enumerate(ids):
            self.metadata_store[id_val] = {
                "text": texts[i],
                "metadata": metadatas[i],
                "vector": vectors[i]
            }

        print(f"Added {len(ids)} items.")

    def search(self, query_vector, k=3):
        query_vector = np.array([query_vector]).astype('float32')
        distances, indices = self.index.search(query_vector, k)

        results = []
        for i in range(k):
            idx = indices[0][i]
            dist = distances[0][i]
            if idx != -1:
                item = self.metadata_store.get(idx)
                if item:
                    results.append({
                        "id": int(idx),
                        "distance": float(dist),
                        "text": item["text"],
                        "metadata": item["metadata"]
                    })
        return results

    def search_by_metadata(self, key, value):
        results = []
        for id_val, item in self.metadata_store.items():
            if item["metadata"].get(key) == value:
                results.append({
                    "id": id_val,
                    "text": item["text"],
                    "metadata": item["metadata"]
                })
        return results

    def update_metadata(self, id_val, new_metadata):
        if id_val in self.metadata_store:
            self.metadata_store[id_val]["metadata"] = new_metadata
            print(f"Updated metadata for ID {id_val}")
            return True
        return False

    def delete(self, id_val):
        # Faiss deletion is tricky. Usually requires remove_ids (IDSelector)
        # Verify if IndexFlatL2 supports remove_ids
        try:
            self.index.remove_ids(np.array([id_val], dtype='int64'))
            del self.metadata_store[id_val]
            print(f"Deleted ID {id_val}")
            return True
        except Exception as e:
            print(f"Deletion failed (Index might not support remove_ids): {e}")
            return False

    def save(self):
        faiss.write_index(self.index, self.index_path)
        with open(self.metadata_path, 'wb') as f:
            pickle.dump(self.metadata_store, f)
        print("Index saved.")

    def load(self):
        if os.path.exists(self.index_path) and os.path.exists(self.metadata_path):
            self.index = faiss.read_index(self.index_path)
            with open(self.metadata_path, 'rb') as f:
                self.metadata_store = pickle.load(f)
            print("Index loaded.")

def main():
    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])
    store = FaissStore(dim)

    # 1. Add Data
    ids = [item["id"] for item in data]
    vectors = [item["vector"] for item in data]
    texts = [item["text"] for item in data]
    metadatas = [item["metadata"] for item in data]

    store.add(ids, vectors, texts, metadatas)

    # 2. Search (Vector)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]
    results = store.search(query_vector, k=3)
    for res in results:
        print(f"ID: {res['id']}, Distance: {res['distance']:.4f}, Text: {res['text']}, Metadata: {res['metadata']}")

    # 3. Search (Metadata)
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    results = store.search_by_metadata("category", "tech")
    for res in results:
        print(f"ID: {res['id']}, Text: {res['text']}, Metadata: {res['metadata']}")

    # 4. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]
    print(f"Before: {store.metadata_store[item_id]['metadata']}")
    store.update_metadata(item_id, {"category": "food"})
    print(f"After: {store.metadata_store[item_id]['metadata']}")

    # 5. Delete
    print("\n--- Deleting Item ---")
    store.delete(item_id)

    # Verify
    if item_id not in store.metadata_store:
        print("Item deleted from metadata store.")

    # Verify search doesn't return it
    results = store.search(query_vector, k=3)
    found = any(r['id'] == item_id for r in results)
    if not found:
        print("Item not found in vector search.")

if __name__ == "__main__":
    main()
