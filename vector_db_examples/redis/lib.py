import redis
import numpy as np
from redis.commands.search.field import TextField, TagField, VectorField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

class RedisDB:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.client = None
        self.index_name = "idx:items"
        self.prefix = "item:"

    def connect(self):
        print(f"Connecting to Redis at {self.host}:{self.port}...")
        self.client = redis.Redis(host=self.host, port=self.port, decode_responses=False)

    def check_ready(self):
        try:
            if not self.client:
                self.connect()
            return self.client.ping()
        except:
            return False

    def setup(self, dim):
        if not self.client:
            self.connect()

        try:
            self.client.ft(self.index_name).info()
            print("Index already exists, dropping it...")
            self.client.ft(self.index_name).dropindex()
        except:
            pass

        schema = (
            TextField("text"),
            TagField("category"),
            VectorField("vector",
                "FLAT", {
                    "TYPE": "FLOAT32",
                    "DIM": dim,
                    "DISTANCE_METRIC": "COSINE"
                }
            )
        )

        definition = IndexDefinition(prefix=[self.prefix], index_type=IndexType.HASH)

        self.client.ft(self.index_name).create_index(schema, definition=definition)
        print(f"Index '{self.index_name}' created.")

    def insert_data(self, data):
        if not self.client:
            self.connect()

        pipeline = self.client.pipeline()
        for item in data:
            key = f"{self.prefix}{item['id']}"
            vector = np.array(item["vector"], dtype=np.float32).tobytes()

            pipeline.hset(key, mapping={
                "text": item["text"],
                "category": item["metadata"]["category"],
                "vector": vector
            })
        pipeline.execute()
        print(f"Added {len(data)} items.")

    def search(self, query_vector, limit=5):
        if not self.client:
            self.connect()

        query_vec_bytes = np.array(query_vector, dtype=np.float32).tobytes()

        q = Query(f"*=>[KNN {limit} @vector $vec AS score]")\
            .sort_by("score")\
            .return_fields("score", "text", "category")\
            .dialect(2)

        params = {"vec": query_vec_bytes}

        res = self.client.ft(self.index_name).search(q, query_params=params)

        results = []
        for doc in res.docs:
            results.append({
                "id": doc.id,
                "score": doc.score,
                "text": doc.text,
                "category": doc.category
            })
        return results

    def delete_data(self, item_id):
        if not self.client:
            self.connect()

        key = f"{self.prefix}{item_id}"
        self.client.delete(key)

    def teardown(self):
        if self.client:
            try:
                self.client.ft(self.index_name).dropindex()
            except:
                pass
