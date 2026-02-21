from elasticsearch import Elasticsearch, NotFoundError

class ElasticsearchDB:
    def __init__(self, host='http://localhost:9200'):
        self.host = host
        self.client = None
        self.index_name = "example_index"

    def connect(self):
        print(f"Connecting to Elasticsearch at {self.host}...")
        self.client = Elasticsearch(self.host)

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

        # Check if index exists
        if self.client.indices.exists(index=self.index_name):
            self.client.indices.delete(index=self.index_name)

        mapping = {
            "properties": {
                "text": {"type": "text"},
                "category": {"type": "keyword"},
                "vector": {
                    "type": "dense_vector",
                    "dims": dim,
                    "index": True,
                    "similarity": "cosine"
                }
            }
        }

        self.client.indices.create(index=self.index_name, mappings=mapping)
        print(f"Index '{self.index_name}' created.")

    def insert_data(self, data):
        if not self.client:
            self.connect()

        for item in data:
            doc = {
                "text": item["text"],
                "category": item["metadata"]["category"],
                "vector": item["vector"]
            }
            self.client.index(index=self.index_name, id=str(item["id"]), document=doc)

        self.client.indices.refresh(index=self.index_name)
        print(f"Indexed {len(data)} documents.")

    def search(self, query_vector, limit=5):
        if not self.client:
            self.connect()

        response = self.client.search(
            index=self.index_name,
            knn={
                "field": "vector",
                "query_vector": query_vector,
                "k": limit,
                "num_candidates": 100
            },
            source=["text", "category"]
        )

        results = []
        for hit in response['hits']['hits']:
            results.append({
                "id": hit['_id'],
                "score": hit['_score'],
                "text": hit['_source']['text'],
                "category": hit['_source']['category']
            })
        return results

    def delete_data(self, item_id):
        if not self.client:
            self.connect()

        try:
            self.client.delete(index=self.index_name, id=str(item_id))
        except NotFoundError:
            pass

    def teardown(self):
        if self.client and self.client.indices.exists(index=self.index_name):
            self.client.indices.delete(index=self.index_name)
