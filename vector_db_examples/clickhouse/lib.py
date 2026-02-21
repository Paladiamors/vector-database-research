import clickhouse_connect
import time

class ClickHouseDB:
    def __init__(self, host='localhost', port=8123, password='default'):
        self.host = host
        self.port = port
        self.password = password
        self.client = None

    def connect(self):
        print(f"Connecting to ClickHouse at {self.host}:{self.port}...")
        self.client = clickhouse_connect.get_client(host=self.host, password=self.password, port=self.port)

    def check_ready(self):
        try:
            if not self.client:
                self.connect()
            self.client.command("SELECT 1")
            return True
        except Exception as e:
            print(f"ClickHouse not ready: {e}")
            return False

    def setup(self, dim):
        if not self.client:
            self.connect()

        self.client.command("DROP TABLE IF EXISTS items")
        self.client.command("SET allow_experimental_vector_similarity_index = 1")

        # vector_similarity('hnsw', 'L2Distance', dim, 'f32', 16, 100)
        # Using f32 for quantization to match Float32 input, M=16, ef_construction=100
        query = f"""
            CREATE TABLE items (
                id Int32,
                text String,
                category String,
                vector Array(Float32),
                INDEX vec_idx vector TYPE vector_similarity('hnsw', 'L2Distance', {dim}, 'f32', 16, 100) GRANULARITY 1000
            ) ENGINE = MergeTree()
            ORDER BY id
        """
        self.client.command(query)
        print("Table 'items' created with vector index.")

    def insert_data(self, data):
        if not self.client:
            self.connect()

        rows = []
        for item in data:
            rows.append([
                item["id"],
                item["text"],
                item["metadata"]["category"],
                item["vector"]
            ])

        self.client.insert("items", rows, column_names=["id", "text", "category", "vector"])
        print(f"Inserted {len(rows)} items.")

    def search(self, query_vector, limit=5):
        if not self.client:
            self.connect()

        query_vec_str = str(query_vector)
        result = self.client.query(f"""
            SELECT id, text, category, L2Distance(vector, {query_vec_str}) as dist
            FROM items
            ORDER BY dist ASC
            LIMIT {limit}
        """)
        return result.result_rows

    def delete_data(self, item_id):
        if not self.client:
            self.connect()

        self.client.command(f"ALTER TABLE items DELETE WHERE id = {item_id}")
        # Wait for mutation
        time.sleep(1)

    def teardown(self):
        if self.client:
            self.client.command("DROP TABLE IF EXISTS items")
