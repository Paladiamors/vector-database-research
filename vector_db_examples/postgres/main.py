import psycopg2
from pgvector.psycopg2 import register_vector
import json
import numpy as np
import os

def main():
    # Connect
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        database="vectordb"
    )
    conn.autocommit = True

    # Register vector extension for psycopg2
    register_vector(conn)

    cur = conn.cursor()

    # 1. Setup Table and Extension
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    cur.execute("DROP TABLE IF EXISTS items")

    # Create table with vector column
    # Use 384 dimensions for all-MiniLM-L6-v2
    cur.execute("""
        CREATE TABLE items (
            id SERIAL PRIMARY KEY,
            text TEXT,
            metadata JSONB,
            embedding VECTOR(384)
        )
    """)
    print("Table created.")

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    # 2. Insert Data
    print(f"Inserting {len(data)} items...")
    for item in data:
        cur.execute(
            "INSERT INTO items (id, text, metadata, embedding) VALUES (%s, %s, %s, %s)",
            (item["id"], item["text"], json.dumps(item["metadata"]), item["vector"])
        )

    # Create Index (IVFFlat)
    # Requires enough rows to be useful, but demonstrating syntax
    # cur.execute("CREATE INDEX ON items USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)")
    # For small dataset, HNSW is also good or just no index (exact search)
    # Using HNSW:
    cur.execute("CREATE INDEX ON items USING hnsw (embedding vector_cosine_ops)")
    print("Index created.")

    # 3. Search (Vector Search)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]

    # Using <=> for cosine distance (or <-> for L2, <#> for negative inner product)
    # vector_cosine_ops uses <=>
    cur.execute("""
        SELECT id, text, metadata, 1 - (embedding <=> %s) as similarity
        FROM items
        ORDER BY embedding <=> %s
        LIMIT 3
    """, (query_vector, query_vector))

    rows = cur.fetchall()
    for row in rows:
        print(f"ID: {row[0]}, Similarity: {row[3]:.4f}, Text: {row[1]}, Metadata: {row[2]}")

    # 4. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    # Using JSONB operator ->>
    cur.execute("""
        SELECT id, text, metadata
        FROM items
        WHERE metadata->>'category' = 'tech'
    """)

    rows = cur.fetchall()
    for row in rows:
        print(f"ID: {row[0]}, Text: {row[1]}, Metadata: {row[2]}")

    # 5. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]

    # Verify before
    cur.execute("SELECT metadata FROM items WHERE id = %s", (item_id,))
    print(f"Before: {cur.fetchone()[0]}")

    # Update JSONB
    new_meta = json.dumps({"category": "food"})
    cur.execute("UPDATE items SET metadata = %s WHERE id = %s", (new_meta, item_id))

    # Verify after
    cur.execute("SELECT metadata FROM items WHERE id = %s", (item_id,))
    print(f"After: {cur.fetchone()[0]}")

    # 6. Delete Item
    print("\n--- Deleting Item ---")
    cur.execute("DELETE FROM items WHERE id = %s", (item_id,))

    # Verify
    cur.execute("SELECT id FROM items WHERE id = %s", (item_id,))
    if cur.fetchone() is None:
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
