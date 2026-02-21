import json
import os
import sys
import weaviate

# Add current directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib import WeaviateDB
import weaviate.classes.query as wvq

def main():
    db = WeaviateDB()

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
        print(f"Text: {res['text']}, Category: {res['category']}, Distance: {res['distance']:.4f}")

    # Metadata Search
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    collection = db.client.collections.get(db.collection_name)
    response = collection.query.fetch_objects(
        filters=wvq.Filter.by_property("category").equal("tech"),
        limit=5
    )
    for o in response.objects:
        print(f"Text: {o.properties['text']}, Category: {o.properties['category']}")

    # Update
    print("\n--- Updating Metadata ---")
    item_id = data[0]["id"]
    item_uuid = weaviate.util.generate_uuid5(item_id)

    collection.data.update(
        uuid=item_uuid,
        properties={"category": "food"}
    )

    obj = collection.query.fetch_object_by_id(item_uuid)
    if obj:
        print(f"After: Category = {obj.properties['category']}")

    # Delete
    print("\n--- Deleting Item ---")
    db.delete_data(item_id)

    obj = collection.query.fetch_object_by_id(item_uuid)
    if obj is None:
        print("Object successfully deleted.")
    else:
        print("Object still exists.")

    db.client.close()

if __name__ == "__main__":
    main()
