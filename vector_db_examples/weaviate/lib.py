import weaviate
import weaviate.classes.config as wvc
from weaviate.classes.query import Filter, MetadataQuery
import json

class WeaviateDB:
    def __init__(self, host='localhost', port=8080, grpc_port=50051):
        self.host = host
        self.port = port
        self.grpc_port = grpc_port
        self.client = None
        self.collection_name = "Document"

    def connect(self):
        print(f"Connecting to Weaviate at {self.host}:{self.port}...")
        self.client = weaviate.connect_to_local(
            port=self.port,
            grpc_port=self.grpc_port
        )

    def check_ready(self):
        try:
            if not self.client:
                self.connect()
            return self.client.is_ready()
        except:
            return False

    def setup(self, dim):
        if not self.client:
            self.connect()

        if self.client.collections.exists(self.collection_name):
            self.client.collections.delete(self.collection_name)

        # Use self_provided() for BYOV
        # Configure.Vectors.self_provided()

        self.client.collections.create(
            name=self.collection_name,
            properties=[
                wvc.Property(name="text", data_type=wvc.DataType.TEXT),
                wvc.Property(name="category", data_type=wvc.DataType.TEXT),
            ],
            # vector_config implies we are providing the vectors ourselves
            vectorizer_config=wvc.Configure.Vectorizer.none(),
            # Wait, docs said vector_config=...Vectors.self_provided() ???
            # Let's try what the docs said for BYOV:
            # vector_config=wvc.Configure.Vectors.self_provided()
            # But the error said invalid type for vector_config.
            # If I use vectorizer_config=none, it might default vector index.
            # If I use vector_config, it expects specific config.
            # Let's try exactly what docs said:
            # vector_config=wvc.Configure.Vectors.self_provided() is likely for NamedVectors?
            # Or default vector?
            # Actually, "vectorizer_config has been replaced with vector_config"
            # And "Configure.Vectorizer.none have been replaced with Configure.Vectors.self_provided"
            # So:
            # vector_config=wvc.Configure.Vectors.self_provided()

            # Wait, wvc is imported as weaviate.classes.config
            vector_config=wvc.Configure.Vectors.self_provided(),
        )
        print(f"Created collection '{self.collection_name}'.")

    def insert_data(self, data):
        if not self.client:
            self.connect()

        collection = self.client.collections.get(self.collection_name)
        print(f"Inserting {len(data)} items...")

        with collection.batch.dynamic() as batch:
            for item in data:
                batch.add_object(
                    properties={
                        "text": item["text"],
                        "category": item["metadata"]["category"]
                    },
                    vector=item["vector"],
                    uuid=weaviate.util.generate_uuid5(item["id"])
                )

        if len(collection.batch.failed_objects) > 0:
            print(f"Failed objects: {len(collection.batch.failed_objects)}")
            for failed in collection.batch.failed_objects:
                print(failed)
        else:
            print("Data inserted.")

    def search(self, query_vector, limit=5):
        if not self.client:
            self.connect()

        collection = self.client.collections.get(self.collection_name)
        response = collection.query.near_vector(
            near_vector=query_vector,
            limit=limit,
            return_metadata=MetadataQuery(distance=True)
        )

        results = []
        for o in response.objects:
            results.append({
                "id": o.uuid, # Note: this is UUID, not original ID unless we map it back
                "text": o.properties['text'],
                "category": o.properties['category'],
                "distance": o.metadata.distance
            })
        return results

    def delete_data(self, item_id):
        if not self.client:
            self.connect()

        collection = self.client.collections.get(self.collection_name)
        uuid = weaviate.util.generate_uuid5(item_id)
        collection.data.delete_by_id(uuid)

    def teardown(self):
        if self.client:
            if self.client.collections.exists(self.collection_name):
                self.client.collections.delete(self.collection_name)
            self.client.close()
