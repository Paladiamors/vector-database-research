import deeplake
import json
import numpy as np
import shutil
import os

def cosine_similarity(v1, v2):
    return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))

def main():
    dataset_path = "./deeplake_db"
    abs_path = os.path.abspath(dataset_path)

    # Clean up previous run
    if os.path.exists(dataset_path):
        shutil.rmtree(dataset_path)

    print(f"Creating Deep Lake dataset at {dataset_path}...")
    ds = deeplake.create(dataset_path)

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])

    # 1. Add Columns (Schema)
    # Renamed 'metadata' to 'meta' to avoid conflict with ds.metadata property
    ds.add_column('ids', deeplake.types.Text)
    ds.add_column('text', deeplake.types.Text)
    ds.add_column('meta', deeplake.types.Dict)
    ds.add_column('embedding', deeplake.types.Embedding(size=dim))

    # 2. Add Data
    print(f"Adding {len(data)} items to dataset.")
    for item in data:
        ds.append({
            'ids': [str(item["id"])],
            'text': [item["text"]],
            'meta': [item["metadata"]],
            'embedding': [np.array(item["vector"], dtype=np.float32)]
        })

    ds.commit() # Commit changes
    print("Data added and committed.")

    # 3. Search (Vector Search via TQL or Python)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = np.array(data[0]["vector"], dtype=np.float32)

    # Try TQL first
    vec_list = query_vector.tolist()
    # Use explicit path in query
    query_string = f"select * from (select *, cosine_similarity(embedding, ARRAY{vec_list}) as score from \"{abs_path}\") order by score desc limit 3"

    try:
        result = ds.query(query_string)
        for i in range(len(result['text'])):
            # Result keys might be 'text', 'ids' etc or aliases
            # Check keys
            print(f"ID: {result['ids'][i]}, Score: {result['score'][i]:.4f}, Text: {result['text'][i]}, Metadata: {result['meta'][i]}")

    except Exception as e:
        print(f"TQL Vector search failed: {e}")
        print("Falling back to Manual Numpy Search.")
        # Load embeddings into memory
        embs = ds['embedding'].numpy()
        scores = []
        for i, emb in enumerate(embs):
            score = cosine_similarity(query_vector, emb)
            scores.append((score, i))

        scores.sort(key=lambda x: x[0], reverse=True)
        top_k = scores[:3]

        for score, i in top_k:
            meta_val = ds['meta'][i]
            print(f"ID: {ds['ids'][i]}, Score: {score:.4f}, Text: {ds['text'][i]}, Metadata: {meta_val}")


    # 4. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")

    # Try TQL
    query_string = f"select * from \"{abs_path}\" where meta['category'] == 'tech'"

    try:
        result = ds.query(query_string)
        if len(result['text']) == 0:
             raise Exception("Empty result or failed filter")
        for i in range(len(result['text'])):
            print(f"ID: {result['ids'][i]}, Text: {result['text'][i]}, Metadata: {result['meta'][i]}")
    except Exception as e:
        print(f"TQL Metadata filtering failed or empty: {e}")
        # Fallback to python iteration
        print("Falling back to Python iteration.")
        for i in range(len(ds)):
            meta = ds['meta'][i]
            if meta.get('category') == 'tech':
                print(f"ID: {ds['ids'][i]}, Text: {ds['text'][i]}")

    # 5. Update Metadata
    print("\n--- Updating Metadata ---")
    id_to_update = str(data[0]["id"])

    idx = -1
    for i in range(len(ds)):
        if ds['ids'][i] == id_to_update:
            idx = i
            break

    if idx != -1:
        current_meta = ds['meta'][idx]
        print(f"Before: {current_meta}")

        new_meta = dict(current_meta)
        new_meta['category'] = 'food'

        try:
            # Update specific column at index
            # In Deep Lake 4.x, we might need to use a specific method to update
            # ds['meta'][idx] = new_meta might work if supported
            ds['meta'][idx] = new_meta
            ds.commit()
            print(f"After: {ds['meta'][idx]}")
        except Exception as e:
            print(f"Update failed: {e}")

    # 6. Delete Item
    print("\n--- Deleting Item ---")

    if idx != -1:
        try:
            # Deep Lake 4.x uses delete(index)
            ds.delete(idx)
            ds.commit()
            print("Item deleted.")
        except Exception as e:
             print(f"Deletion failed: {e}")

    # Verify
    found = False
    # If pop works, length decreases
    # Re-scan
    for i in range(len(ds)):
        if ds['ids'][i] == id_to_update:
             found = True
             break
    if not found:
        print("Item successfully deleted (verified).")

if __name__ == "__main__":
    main()
