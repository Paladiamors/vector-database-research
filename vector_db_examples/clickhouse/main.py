import json
import os
import sys

# Add current directory to sys.path to ensure lib import works
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import ClickHouseDB

def main():
    db = ClickHouseDB()

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

    for row in results:
        print(f"ID: {row[0]}, Text: {row[1]}, Category: {row[2]}, Distance: {row[3]:.4f}")

    # Metadata Search (Raw query as lib doesn't abstract it yet, or use generic query)
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    res = db.client.query("SELECT id, text, category FROM items WHERE category = 'tech'")
    for row in res.result_rows:
        print(f"ID: {row[0]}, Text: {row[1]}, Category: {row[2]}")

    # Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]
    db.client.command(f"ALTER TABLE items UPDATE category = 'food' WHERE id = {item_id}")
    import time
    time.sleep(1)

    after = db.client.query(f"SELECT category FROM items WHERE id = {item_id}").result_rows[0][0]
    print(f"After: {after}")

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_id)

    count = db.client.query(f"SELECT count() FROM items WHERE id = {item_id}").result_rows[0][0]
    if count == 0:
        print("Item successfully deleted.")
    else:
        print("Item still exists (mutation pending).")

if __name__ == "__main__":
    main()
