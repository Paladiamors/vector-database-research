from pymilvus import (
    connections,
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)

class MilvusDB:
    def __init__(self, host='localhost', port='19530'):
        self.host = host
        self.port = port
        self.collection_name = "example_collection"
        self.alias = "default"

    def connect(self):
        print(f"Connecting to Milvus at {self.host}:{self.port}...")
        connections.connect(self.alias, host=self.host, port=self.port)

    def check_ready(self):
        try:
            self.connect()
            return True
        except:
            return False

    def setup(self, dim):
        self.connect()

        if utility.has_collection(self.collection_name):
            utility.drop_collection(self.collection_name)

        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="category", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=1000)
        ]

        schema = CollectionSchema(fields, "Example collection for vector search")
        collection = Collection(self.collection_name, schema)
        print(f"Collection '{self.collection_name}' created.")

        # Create index immediately or after insert?
        # Usually better to create index after insert for bulk, but Milvus handles dynamic.
        # We will create index after insert.

    def insert_data(self, data):
        self.connect()
        collection = Collection(self.collection_name)

        ids = [item["id"] for item in data]
        vectors = [item["vector"] for item in data]
        categories = [item["metadata"]["category"] for item in data]
        texts = [item["text"] for item in data]

        entities = [
            ids,
            vectors,
            categories,
            texts
        ]

        insert_result = collection.insert(entities)
        # Use insert_count if available, else primary_keys length
        count = getattr(insert_result, 'insert_count', len(insert_result.primary_keys))
        print(f"Inserted {count} entities.")

        # Create Index
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024}
        }
        collection.create_index(field_name="vector", index_params=index_params)
        collection.load()

    def search(self, query_vector, limit=5):
        self.connect()
        collection = Collection(self.collection_name)
        collection.load() # Ensure loaded

        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}

        results = collection.search(
            data=[query_vector],
            anns_field="vector",
            param=search_params,
            limit=limit,
            output_fields=["text", "category"]
        )

        output = []
        for hits in results:
            for hit in hits:
                output.append({
                    "id": hit.id,
                    "distance": hit.distance,
                    "text": hit.entity.get('text'),
                    "category": hit.entity.get('category')
                })
        return output

    def delete_data(self, item_id):
        self.connect()
        collection = Collection(self.collection_name)
        expr = f"id == {item_id}"
        collection.delete(expr)

    def teardown(self):
        self.connect()
        if utility.has_collection(self.collection_name):
            utility.drop_collection(self.collection_name)
