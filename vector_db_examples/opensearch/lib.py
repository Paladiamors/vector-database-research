from opensearchpy import OpenSearch, NotFoundError

class OpenSearchDB:
    def __init__(self, host='localhost', port=9200):
        self.host = host
        self.port = port
        self.client = None
        self.index_name = "example_index"

    def connect(self):
        print(f"Connecting to OpenSearch at {self.host}:{self.port}...")
        self.client = OpenSearch(
            hosts=[{'host': self.host, 'port': self.port}],
            http_compress=True,
            use_ssl=False
        )

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

        if self.client.indices.exists(index=self.index_name):
            self.client.indices.delete(index=self.index_name)

        index_body = {
            "settings": {
                "index": {
                    "knn": True
                }
            },
            "mappings": {
                "properties": {
                    "vector": {
                        "type": "knn_vector",
                        "dimension": dim,
                        "method": {
                             "name": "hnsw",
                             "engine": "nmslib"
                        }
                    },
                    "text": {"type": "text"},
                    "category": {"type": "keyword"}
                }
            }
        }

        self.client.indices.create(index=self.index_name, body=index_body)
        print(f"Index '{self.index_name}' created.")

    def insert_data(self, data):
        if not self.client:
            self.connect()

        for item in data:
            doc = {
                "vector": item["vector"],
                "text": item["text"],
                "category": item["metadata"]["category"]
            }
            self.client.index(index=self.index_name, id=str(item["id"]), body=doc)

        self.client.indices.refresh(index=self.index_name)
        print(f"Indexed {len(data)} documents.")

    def search(self, query_vector, limit=5):
        if not self.client:
            self.connect()

        query = {
            "size": limit,
            "query": {
                "knn": {
                    "vector": {
                        "vector": query_vector,
                        "k": limit
                    }
                }
            }
        }

        response = self.client.search(index=self.index_name, body=query)

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
