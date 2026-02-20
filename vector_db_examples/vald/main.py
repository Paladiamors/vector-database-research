import grpc
import json
import os
import numpy as np

# Assuming vald-client-python is installed and generated correctly
try:
    from vald.v1.vald import vald_pb2_grpc
    from vald.v1.payload import payload_pb2
except ImportError:
    print("Vald client libraries not found. Please install vald-client-python.")
    exit(1)

def main():
    host = 'localhost:8081'
    print(f"Connecting to Vald at {host}...")

    channel = grpc.insecure_channel(host)
    stub = vald_pb2_grpc.ValdStub(channel)

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    dim = len(data[0]["vector"])

    # 1. Insert Data
    print(f"Inserting {len(data)} items...")

    # Vald Insert is usually stream or unary
    # We use Insert (unary) in loop or StreamInsert

    for item in data:
        vec = payload_pb2.Object.Vector(id=str(item["id"]), vector=item["vector"])
        # Metadata handling in Vald is via 'ips' (Ingress/Egress filter) or external.
        # Vald core is vector index.
        # But for this example, we just index the vector and ID.
        # We can simulate metadata by storing it elsewhere or using ID to look up.
        # The table says "Metadata Support: Distributed". This might refer to Vald's architecture.
        # Vald Filter Gateway supports metadata filtering.
        # We are using Vald Agent NGT directly here (standalone).
        # Standalone might not support metadata filtering fully.

        req = payload_pb2.Insert.Request(vector=vec, config=payload_pb2.Insert.Config(skip_strict_exist_check=True))
        stub.Insert(req)

    print("Data inserted.")

    # Wait for indexing (Vald is eventually consistent / auto-commit)
    import time
    time.sleep(5)

    # 2. Search (Vector Search)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]

    # Config
    cfg = payload_pb2.Search.Config(num=3, radius=-1.0, epsilon=0.01, timeout=3000000000) # 3s

    req = payload_pb2.Search.Request(vector=query_vector, config=cfg)

    try:
        res = stub.Search(req)
        for hit in res.results:
             print(f"ID: {hit.id}, Distance: {hit.distance}")
             # We would fetch metadata from external store using ID
    except grpc.RpcError as e:
        print(f"Search failed: {e}")

    # 3. Metadata Search (Not natively supported in standalone agent without gateway filters)
    print("\n--- Metadata Search Results ---")
    print("Metadata search in Vald usually requires Vald Gateway with Filter or external metadata store.")
    print("Skipping metadata search example for standalone agent.")

    # 4. Update
    print("\n--- Updating Item ---")
    # Update vector for item 1
    # Vald Update replaces the vector
    item_id = str(data[0]["id"])
    new_vector = data[0]["vector"] # Same vector for demo

    vec = payload_pb2.Object.Vector(id=item_id, vector=new_vector)
    req = payload_pb2.Update.Request(vector=vec, config=payload_pb2.Update.Config(skip_strict_exist_check=True))

    try:
        stub.Update(req)
        print(f"Updated item {item_id}")
    except grpc.RpcError as e:
        print(f"Update failed: {e}")

    # 5. Delete Item
    print("\n--- Deleting Item ---")
    id_req = payload_pb2.Object.ID(id=item_id)
    req = payload_pb2.Remove.Request(id=id_req, config=payload_pb2.Remove.Config(skip_strict_exist_check=True))

    try:
        stub.Remove(req)
        print(f"Removed item {item_id}")
    except grpc.RpcError as e:
        print(f"Remove failed: {e}")

if __name__ == "__main__":
    main()
