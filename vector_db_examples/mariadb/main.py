import json
import os
import sys

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from lib import MariaDB
except ImportError:
    print("Could not import MariaDB lib. Ensure mariadb package is installed.")
    sys.exit(1)

def main():
    try:
        db = MariaDB()
    except ImportError:
        print("mariadb package not installed. Skipping MariaDB example.")
        return

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])

    # Setup
    try:
        db.setup(dim)
    except Exception as e:
        print(f"Setup failed: {e}")
        return

    # Insert
    db.insert_data(data)

    # Search
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]
    results = db.search(query_vector, limit=3)

    for row in results:
        # id, text, category, dist
        print(f"ID: {row[0]}, Text: {row[1]}, Category: {row[2]}, Distance: {row[3]:.4f}")

    # Metadata Search
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    # For metadata search we can use generic SQL
    if db.conn:
        cur = db.conn.cursor()
        cur.execute("SELECT id, text, category FROM items WHERE category = 'tech'")
        for row in cur:
            print(f"ID: {row[0]}, Text: {row[1]}, Category: {row[2]}")
        cur.close()

    # Update
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]
    if db.conn:
        cur = db.conn.cursor()
        cur.execute("UPDATE items SET category = 'food' WHERE id = ?", (item_id,))

        cur.execute("SELECT category FROM items WHERE id = ?", (item_id,))
        print(f"After: {cur.fetchone()[0]}")
        cur.close()

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_id)

    if db.conn:
        cur = db.conn.cursor()
        cur.execute("SELECT count(*) FROM items WHERE id = ?", (item_id,))
        if cur.fetchone()[0] == 0:
            print("Item successfully deleted.")
        else:
            print("Item still exists.")
        cur.close()

    db.conn.close()

if __name__ == "__main__":
    main()
