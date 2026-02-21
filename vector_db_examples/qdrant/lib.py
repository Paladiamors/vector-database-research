from qdrant_client import QdrantClient
from qdrant_client.http import models
import json

class QdrantDB:
    def __init__(self, host='localhost', port=6333):
        self.host = host
        self.port = port
        self.client = None
        self.collection_name = "example_collection"

    def connect(self):
        print(f"Connecting to Qdrant at {self.host}:{self.port}...")
        self.client = QdrantClient(host=self.host, port=self.port)

    def check_ready(self):
        try:
            if not self.client:
                self.connect()
            self.client.get_collections()
            return True
        except:
            return False

    def setup(self, dim):
        if not self.client:
            self.connect()

        if self.client.collection_exists(self.collection_name):
            self.client.delete_collection(self.collection_name)

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE)
        )
        print(f"Created collection '{self.collection_name}' with dimension {dim}.")

    def insert_data(self, data):
        if not self.client:
            self.connect()

        points = []
        for item in data:
            points.append(models.PointStruct(
                id=item["id"],
                vector=item["vector"],
                payload={
                    "text": item["text"],
                    "category": item["metadata"]["category"]
                }
            ))

        # Upsert
        operation_info = self.client.upsert(
            collection_name=self.collection_name,
            wait=True,
            points=points
        )
        print(f"Upsert status: {operation_info.status}")

    def search(self, query_vector, limit=5):
        if not self.client:
            self.connect()

        # Use search (or query_points if needed)
        search_result = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            limit=limit
        )

        results = []
        for hit in search_result:
            results.append({
                "id": hit.id,
                "score": hit.score,
                "payload": hit.payload
            })
        return results

    def delete_data(self, item_id):
        if not self.client:
            self.connect()

        self.client.delete(
            collection_name=self.collection_name,
            points_selector=models.PointIdsList(points=[item_id]),
            wait=True
        )

    def teardown(self):
        if self.client and self.client.collection_exists(self.collection_name):
            self.client.delete_collection(self.collection_name)
