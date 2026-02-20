from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import json
import os
import time

def main():
    # Connect
    print("Connecting to Cassandra...")
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])
    keyspace = "vectordb"

    # 1. Create Keyspace
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
    """)
    session.set_keyspace(keyspace)

    # 2. Create Table
    session.execute("DROP TABLE IF EXISTS items")
    session.execute(f"""
        CREATE TABLE items (
            id int PRIMARY KEY,
            text text,
            category text,
            embedding vector<float, {dim}>
        )
    """)
    print("Table created.")

    # 3. Create Index (SAI)
    session.execute("""
        CREATE CUSTOM INDEX IF NOT EXISTS embedding_index ON items(embedding) USING 'StorageAttachedIndex'
    """)
    # Also index category for filtering
    session.execute("""
        CREATE CUSTOM INDEX IF NOT EXISTS category_index ON items(category) USING 'StorageAttachedIndex'
    """)
    print("Indices created.")

    # 4. Insert Data
    print(f"Inserting {len(data)} items...")
    prepared = session.prepare("INSERT INTO items (id, text, category, embedding) VALUES (?, ?, ?, ?)")

    for item in data:
        # embedding expects list of floats directly? Or string?
        # Cassandra driver 3.29+ supports vector type directly as list/array
        session.execute(prepared, (item["id"], item["text"], item["metadata"]["category"], item["vector"]))

    print("Data inserted.")

    # 5. Search (Vector Search via ANN)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]

    # ANN OF syntax
    # Note: query_vector placeholder works with list
    # Use simple statement or prepared?
    # Simple statement might fail parsing if query_vector is too long.
    # Prepared statement works best.

    # query: SELECT * FROM items ORDER BY embedding ANN OF ? LIMIT 3
    stmt = session.prepare("SELECT id, text, category FROM items ORDER BY embedding ANN OF ? LIMIT 3")
    rows = session.execute(stmt, (query_vector,))

    for row in rows:
        print(f"ID: {row.id}, Text: {row.text}, Category: {row.category}")

    # 6. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    # ANN search combined with filter
    # SELECT * FROM items WHERE category = 'tech' ORDER BY embedding ANN OF ? LIMIT 3 ALLOW FILTERING?
    # Or just pure filter?
    # Prompt asks "search by meta data".

    # Pure filter:
    rows = session.execute("SELECT id, text, category FROM items WHERE category = 'tech'")
    for row in rows:
        print(f"ID: {row.id}, Text: {row.text}, Category: {row.category}")

    # 7. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]

    # Verify before
    row = session.execute("SELECT category FROM items WHERE id = %s", (item_id,)).one()
    print(f"Before: {row.category}")

    # Update
    session.execute("UPDATE items SET category = 'food' WHERE id = %s", (item_id,))

    # Verify after
    row = session.execute("SELECT category FROM items WHERE id = %s", (item_id,)).one()
    print(f"After: {row.category}")

    # 8. Delete Item
    print("\n--- Deleting Item ---")
    session.execute("DELETE FROM items WHERE id = %s", (item_id,))

    # Verify
    row = session.execute("SELECT id FROM items WHERE id = %s", (item_id,)).one()
    if row is None:
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

    cluster.shutdown()

if __name__ == "__main__":
    main()
