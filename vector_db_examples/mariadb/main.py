import mysql.connector
import json
import os
import numpy as np

def main():
    # Connect
    print("Connecting to MariaDB...")
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="vectordb"
    )
    cursor = conn.cursor()

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])

    # 1. Create Table
    cursor.execute("DROP TABLE IF EXISTS items")
    # MariaDB 11.4 supports VECTOR type
    cursor.execute(f"""
        CREATE TABLE items (
            id INT PRIMARY KEY,
            text TEXT,
            category VARCHAR(255),
            embedding VECTOR({dim})
        ) ENGINE=InnoDB
    """)
    print("Table created.")

    # 2. Create Vector Index
    # Syntax might vary slightly depending on preview status
    # Standard syntax: ALTER TABLE t ADD VECTOR INDEX (col)
    try:
        cursor.execute("CREATE VECTOR INDEX vec_idx ON items(embedding)")
        print("Vector index created.")
    except Exception as e:
        print(f"Vector index creation failed (might be implicit or not supported yet): {e}")

    # 3. Insert Data
    print(f"Inserting {len(data)} items...")
    sql = "INSERT INTO items (id, text, category, embedding) VALUES (%s, %s, %s, VEC_FromText(%s))"

    for item in data:
        vec_str = str(item["vector"]) # Format as string '[1.0, 2.0, ...]'
        cursor.execute(sql, (item["id"], item["text"], item["metadata"]["category"], vec_str))

    conn.commit()
    print("Data inserted.")

    # 4. Search (Vector Search via Distance)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = str(data[0]["vector"])

    # Using VEC_DISTANCE_EUCLIDEAN (L2) or VEC_DISTANCE_COSINE?
    # Usually euclidean is default distance for vector search
    # If using Cosine similarity, use VEC_DISTANCE_COSINE? Or 1 - cosine
    # MariaDB 11.4 usually supports VEC_DISTANCE

    try:
        cursor.execute(f"""
            SELECT id, text, category, VEC_DISTANCE(embedding, VEC_FromText(%s)) as dist
            FROM items
            ORDER BY dist ASC
            LIMIT 3
        """, (query_vector,))

        rows = cursor.fetchall()
        for row in rows:
            print(f"ID: {row[0]}, Distance: {row[3]:.4f}, Text: {row[1]}, Category: {row[2]}")

    except Exception as e:
        print(f"Vector search failed: {e}")

    # 5. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    cursor.execute("SELECT id, text, category FROM items WHERE category = 'tech'")

    rows = cursor.fetchall()
    for row in rows:
        print(f"ID: {row[0]}, Text: {row[1]}, Category: {row[2]}")

    # 6. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]

    # Verify before
    cursor.execute("SELECT category FROM items WHERE id = %s", (item_id,))
    print(f"Before: {cursor.fetchone()[0]}")

    # Update
    cursor.execute("UPDATE items SET category = 'food' WHERE id = %s", (item_id,))
    conn.commit()

    # Verify after
    cursor.execute("SELECT category FROM items WHERE id = %s", (item_id,))
    print(f"After: {cursor.fetchone()[0]}")

    # 7. Delete Item
    print("\n--- Deleting Item ---")
    cursor.execute("DELETE FROM items WHERE id = %s", (item_id,))
    conn.commit()

    # Verify
    cursor.execute("SELECT id FROM items WHERE id = %s", (item_id,))
    if cursor.fetchone() is None:
        print("Item successfully deleted.")
    else:
        print("Item still exists.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
