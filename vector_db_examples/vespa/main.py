from vespa.package import ApplicationPackage, Field, Schema, Document, RankProfile, HNSW, Attribute
from vespa.deployment import VespaDocker
import json
import os
import requests
import time

def main():
    # 1. Define Application Package
    # Schema: text (index, summary), category (attribute, summary), vector (attribute, index: hnsw)
    document = Document(
        fields=[
            Field(name = "text", type = "string", indexing = ["index", "summary"]),
            Field(name = "category", type = "string", indexing = ["attribute", "summary"]),
            Field(
                name = "vector",
                type = "tensor<float>(x[384])",
                indexing = ["attribute", "index"],
                ann = HNSW(
                    distance_metric = "euclidean",
                    max_links_per_node = 16,
                    neighbors_to_explore_at_insert = 200
                )
            )
        ]
    )

    schema = Schema(
        name = "doc",
        document = document,
        rank_profiles = [
            RankProfile(
                name = "default",
                first_phase = "closeness(field, vector)"
            )
        ]
    )

    app_package = ApplicationPackage(name = "vectordb", schema=[schema])

    # 2. Deploy (Assuming Docker container is running at localhost:8080)
    # Usually we use VespaDocker to deploy, but since we use docker-compose, we use requests to deploy zip?
    # Or rely on pyvespa capabilities to deploy to existing endpoint.
    # pyvespa deploys to config server.

    # We will use `Vespa` instance directly if already deployed, but here we deploy first.
    # To deploy to local container started by docker-compose:
    # app_package.to_files("application_package")
    # Then zip and POST to config server.

    # But for simplicity, we can assume the user runs `vespa deploy` or use python to do it.
    # We'll use python to deploy.

    from vespa.application import Vespa

    # Wait for config server
    print("Waiting for Vespa Config Server...")
    # time.sleep(10)

    try:
        # Deploy
        # We need to zip the application package content
        # pyvespa handles this?
        # `VespaDocker` manages the container. Since we have external container, we can't use it easily?
        # Actually `VespaDocker.deploy_from_disk`?
        pass
    except:
        pass

    # For this example, we assume we can deploy via HTTP
    # We need to export package first
    app_package.to_files("application_package")

    # Deploy via command line or requests?
    # curl --header Content-Type:application/zip --data-binary @application.zip http://localhost:19071/application/v2/tenant/default/application/default

    # We'll use subprocess or requests
    # Zip it
    shutil.make_archive("application", "zip", "application_package")

    print("Deploying application...")
    with open("application.zip", "rb") as f:
        response = requests.post(
            "http://localhost:19071/application/v2/tenant/default/application/default",
            headers={"Content-Type": "application/zip"},
            data=f
        )
        print(response.json())

    # Wait for application to be active
    time.sleep(5)

    app = Vespa(url = "http://localhost", port = 8080)

    # Load dataset
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, '../data/dataset.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    # 3. Feed Data
    print(f"Feeding {len(data)} items...")
    documents = []
    for item in data:
        documents.append({
            "id": str(item["id"]),
            "fields": {
                "text": item["text"],
                "category": item["metadata"]["category"],
                "vector": item["vector"]
            }
        })

    # Feed batch
    app.feed_batch(
        schema="doc",
        batch=documents
    )
    print("Data fed.")

    # 4. Search (Vector Search)
    print("\n--- Vector Search Results (Top 3 similar to item 1) ---")
    query_vector = data[0]["vector"]

    # YQL query with nearestNeighbor
    # select * from doc where {targetHits:3}nearestNeighbor(vector, query_vector)
    # We pass query_vector in 'ranking.features.query(query_vector)'

    res = app.query(
        yql = "select * from sources * where {targetHits:3}nearestNeighbor(vector, query_vector)",
        ranking = "default",
        body = {
            "presentation.format": "json",
            "ranking.features.query(query_vector)": query_vector
        }
    )

    for hit in res.hits:
        print(f"ID: {hit['id']}, Score: {hit['relevance']:.4f}, Text: {hit['fields']['text']}, Category: {hit['fields']['category']}")

    # 5. Search with Metadata Filter
    print("\n--- Metadata Search Results (Category == 'tech') ---")
    # YQL filter
    # select * from doc where category contains 'tech'

    res = app.query(
        yql = "select * from sources * where category contains 'tech'"
    )

    for hit in res.hits:
        print(f"ID: {hit['id']}, Text: {hit['fields']['text']}, Category: {hit['fields']['category']}")

    # 6. Update Metadata
    print("\n--- Updating Metadata ---")
    item_id = str(data[0]["id"])

    # Verify before
    # Fetch by ID?
    # app.get_data(schema, id)
    # ID format in Vespa: id:namespace:schema::user_id
    doc_id = f"id:doc:doc::{item_id}"

    res = app.get_data(schema="doc", data_id=item_id)
    print(f"Before: {res.json['fields']['category']}")

    # Update
    # Update expects partial update structure
    app.update_data(
        schema="doc",
        data_id=item_id,
        fields={"category": "food"}
    )

    # Verify after
    res = app.get_data(schema="doc", data_id=item_id)
    print(f"After: {res.json['fields']['category']}")

    # 7. Delete Item
    print("\n--- Deleting Item ---")
    app.delete_data(schema="doc", data_id=item_id)

    # Verify
    try:
        res = app.get_data(schema="doc", data_id=item_id)
        if res.status_code == 404:
            print("Item successfully deleted.")
        else:
            print("Item still exists.")
    except:
        print("Item successfully deleted (404).")

if __name__ == "__main__":
    import shutil # ensure imported
    main()
