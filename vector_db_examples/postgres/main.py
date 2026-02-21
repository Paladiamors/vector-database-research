import json
import os
import sys

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import PostgresDB

def main():
    db = PostgresDB()

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
        print(f"ID: {res['id']}, Similarity: {res['similarity']:.4f}, Text: {res['text']}, Metadata: {res['metadata']}")

    # Metadata Search
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    if db.conn:
        cur = db.conn.cursor()
        cur.execute("""
            SELECT id, text, metadata
            FROM items
            WHERE metadata->>'category' = 'tech'
        """)
        for row in cur:
            print(f"ID: {row[0]}, Text: {row[1]}, Metadata: {row[2]}")
        cur.close()

    # Update
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]
    if db.conn:
        cur = db.conn.cursor()
        new_meta = json.dumps({"category": "food"})
        cur.execute("UPDATE items SET metadata = %s WHERE id = %s", (new_meta, item_id))

        cur.execute("SELECT metadata FROM items WHERE id = %s", (item_id,))
        print(f"After: {cur.fetchone()[0]}")
        cur.close()

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_id)

    if db.conn:
        cur = db.conn.cursor()
        cur.execute("SELECT id FROM items WHERE id = %s", (item_id,))
        if cur.fetchone() is None:
            print("Item successfully deleted.")
        else:
            print("Item still exists.")
        cur.close()

    db.conn.close()

if __name__ == "__main__":
    main()
