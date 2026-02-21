import json
import os
import sys

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import ValdDB

try:
    from vald.v1.payload import payload_pb2
except ImportError:
    pass

def main():
    try:
        db = ValdDB()
        db.connect()
    except ImportError:
        print("Vald libraries not found. Skipping example.")
        return

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
        print(f"ID: {res['id']}, Distance: {res['distance']:.4f}")

    # Metadata Search - Skipped as Vald standalone doesn't support metadata filtering easily

    # Update
    print("\n--- Updating Item ---")
    item_id = str(data[0]["id"])
    if db.stub and payload_pb2:
        vec = payload_pb2.Object.Vector(id=item_id, vector=data[0]["vector"])
        req = payload_pb2.Update.Request(vector=vec, config=payload_pb2.Update.Config(skip_strict_exist_check=True))
        try:
            db.stub.Update(req)
            print(f"Updated item {item_id}")
        except Exception as e:
            print(f"Update failed: {e}")

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_id)
    print("Item removed.")

if __name__ == "__main__":
    main()
