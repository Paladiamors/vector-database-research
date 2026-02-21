from vespa.package import ApplicationPackage, Field, Schema, Document, RankProfile, HNSW
from vespa.application import Vespa
import requests
import time
import shutil
import os

class VespaDB:
    def __init__(self, host='http://localhost', port=8080, config_port=19071):
        self.host = host
        self.port = port
        self.config_port = config_port
        self.app = None

    def connect(self):
        print(f"Connecting to Vespa at {self.host}:{self.port}...")
        self.app = Vespa(url=self.host, port=self.port)

    def check_ready(self):
        try:
            if not self.app:
                self.connect()
            status = self.app.get_application_status()
            return status is not None
        except:
            return False

    def setup(self, dim):
        # Define Schema
        document = Document(
            fields=[
                Field(name="text", type="string", indexing=["index", "summary"]),
                Field(name="category", type="string", indexing=["attribute", "summary"]),
                Field(
                    name="vector",
                    type=f"tensor<float>(x[{dim}])",
                    indexing=["attribute", "index"],
                    ann=HNSW(
                        distance_metric="euclidean",
                        max_links_per_node=16,
                        neighbors_to_explore_at_insert=200
                    )
                )
            ]
        )

        schema = Schema(
            name="doc",
            document=document,
            rank_profiles=[
                RankProfile(
                    name="default",
                    first_phase="closeness(field, vector)"
                )
            ]
        )

        app_package = ApplicationPackage(name="vectordb", schema=[schema])

        # Export package
        pkg_path = os.path.join(os.path.dirname(__file__), "application_package")
        app_package.to_files(pkg_path)

        # Zip it
        zip_path = shutil.make_archive("application", "zip", pkg_path)

        # Deploy
        print("Deploying application...")
        # URL for prepare and activate
        url = f"{self.host}:{self.config_port}/application/v2/tenant/default/prepareandactivate"

        headers = {"Content-Type": "application/zip"}
        try:
            with open(zip_path, "rb") as f:
                response = requests.post(url, headers=headers, data=f)
            print(f"Deployment response: {response.json()}")
        except Exception as e:
            print(f"Deployment failed: {e}")

        # Wait for application to be active
        time.sleep(10)
        self.connect()

    def insert_data(self, data):
        if not self.app:
            self.connect()

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

        print(f"Feeding {len(data)} items...")

        def callback(response, id):
            if not response.is_successful():
                print(f"Failed to feed {id}: {response.json}")

        self.app.feed_iterable(
            schema="doc",
            iter=documents, # Changed from iter_data to iter
            callback=callback
        )
        print("Data fed.")

    def search(self, query_vector, limit=5):
        if not self.app:
            self.connect()

        res = self.app.query(
            yql=f"select * from sources * where {{targetHits:{limit}}}nearestNeighbor(vector, query_vector)",
            ranking="default",
            body={
                "presentation.format": "json",
                "ranking.features.query(query_vector)": query_vector
            }
        )

        results = []
        if res.hits:
            for hit in res.hits:
                if 'fields' in hit:
                    results.append({
                        "id": hit['id'],
                        "score": hit.get('relevance', 0),
                        "text": hit['fields'].get('text'),
                        "category": hit['fields'].get('category')
                    })
        return results

    def delete_data(self, item_id):
        if not self.app:
            self.connect()

        self.app.delete_data(schema="doc", data_id=str(item_id))

    def teardown(self):
        # We generally don't undeploy in teardown for local dev unless needed
        pass
