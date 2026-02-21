import clickhouse_connect
import json
import os
import numpy as np


def main():
    # Connect
    print("Connecting to ClickHouse...")
    client = clickhouse_connect.get_client(host='localhost', password="default", port=8123)

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])

    # 1. Create Table
    client.command("DROP TABLE IF EXISTS items")

    # Create table with Array(Float32) for vectors
    # Note: Enable experimental vector search indices if needed via settings
    # SET allow_experimental_vector_similarity_index = 1
    client.command("SET allow_experimental_vector_similarity_index = 1")

    client.command(f"""
        CREATE TABLE items (
            id Int32,
            text String,
            category String,
            vector Array(Float32),
            INDEX vec_idx vector TYPE vector_similarity('L2', 'HNSW', 'm=16', 'ef_construction=100') GRANULARITY 1000
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    print("Table created.")

    # 2. Insert Data
    print(f"Inserting {len(data)} items...")
    # Prepare data for batch insert
    # data format for clickhouse-connect: [[row1], [row2], ...]
    rows = []
    for item in data:
        rows.append([
            item["id"],
            item["text"],
            item["metadata"]["category"],
            item["vector"]
        ])

    client.insert("items", rows, column_names=["id", "text", "category", "vector"])
    print("Data inserted.")

    # 3. Search (Vector Search via Distance)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]

    # Use L2Distance function.
    # query_vector needs to be passed as parameter? Or string literal?
    # clickhouse-connect supports parameterized queries nicely?
    # Or format the array string.

    # For parameterized query with array:
    # Use python list directly.
    # formatting list as string for simplicity
    query_vec_str = str(query_vector)

    result = client.query(f"""
        SELECT id, text, category, L2Distance(vector, {query_vec_str}) as dist
        FROM items
        ORDER BY dist ASC
        LIMIT 3
    """)

    for row in result.result_rows:
        print(f"ID: {row[0]}, Text: {row[1]}, Category: {row[2]}, Distance: {row[3]:.4f}")

    # 4. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    # Standard SQL filter
    # But usually combined with vector search via pre-filtering (WHERE)

    result = client.query("""
        SELECT id, text, category
        FROM items
        WHERE category = 'tech'
    """)

    for row in result.result_rows:
        print(f"ID: {row[0]}, Text: {row[1]}, Category: {row[2]}")

    # 5. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]

    # Verify before
    before = client.query(f"SELECT category FROM items WHERE id = {item_id}").result_rows[0][0]
    print(f"Before: {before}")

    # ClickHouse update is heavy (ALTER TABLE UPDATE).
    # For small updates, it's asynchronous mutation.
    client.command(f"ALTER TABLE items UPDATE category = 'food' WHERE id = {item_id}")

    # Wait for mutation? Or query immediately (might not reflect immediately unless mutation finishes).
    # We can check mutations table or just wait a bit.
    import time
    time.sleep(1)  # Simple wait

    # Verify after
    after = client.query(f"SELECT category FROM items WHERE id = {item_id}").result_rows[0][0]
    print(f"After: {after}")

    # 6. Delete Item
    print("\n--- Deleting Item ---")
    client.command(f"ALTER TABLE items DELETE WHERE id = {item_id}")

    # Wait for mutation
    time.sleep(1)

    # Verify
    count = client.query(f"SELECT count() FROM items WHERE id = {item_id}").result_rows[0][0]
    if count == 0:
        print("Item successfully deleted.")
    else:
        print("Item still exists (mutation pending).")


if __name__ == "__main__":
    main()
