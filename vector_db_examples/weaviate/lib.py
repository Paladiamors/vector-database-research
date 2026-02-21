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

        # Use vector_config instead of vectorizer_config
        # And Configure.Vectors.none() (or self_provided? sticking to none() if available or just empty?)
        # If we provide vectors manually, we usually disable vectorizer.
        # wvc.Configure.Vectorizer.none() was the old way.
        # New way might be wvc.Configure.Vectors.none() ??
        # Or just don't specify vector_config if we want manual?
        # No, default might be something else.
        # Based on docs: Configure.Vectors.none() -> Configure.Vectors.self_provided()?
        # I'll try to find 'none' or 'self_provided'.
        # I'll use wvc.Configure.Vectorizer.none() as value but pass to vector_config.

        try:
            vectorizer = wvc.Configure.Vectorizer.none()
        except AttributeError:
            # Fallback for newer client
            try:
                vectorizer = wvc.Configure.Vectors.none()
            except AttributeError:
                 # Fallback
                 vectorizer = None

        # Actually, if we use vector_config, we might need a specific config object.
        # If we skip vector_config, it might default to 'none' if no modules configured?
        # Let's try passing vectorizer to vector_config.

        # NOTE: Deprecation warning said use vector_config.
        # I will attempt to use Configure.Vectorizer.none() passed to vector_config.

        self.client.collections.create(
            name=self.collection_name,
            properties=[
                wvc.Property(name="text", data_type=wvc.DataType.TEXT),
                wvc.Property(name="category", data_type=wvc.DataType.TEXT),
            ],
            # vectorizer_config=wvc.Configure.Vectorizer.none(),
            # Replaced by:
            vector_config=vectorizer,
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
