import psycopg2
from pgvector.psycopg2 import register_vector
import json

class PostgresDB:
    def __init__(self, host="localhost", port=5432, user="postgres", password="password", database="vectordb"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        print("Connecting to PostgreSQL...")
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.conn.autocommit = True

    def check_ready(self):
        try:
            if not self.conn:
                self.connect()
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return True
        except:
            return False

    def setup(self, dim):
        if not self.conn:
            self.connect()

        cur = self.conn.cursor()

        # 1. Create Extension FIRST
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        # 2. Register vector AFTER extension exists
        register_vector(self.conn)

        cur.execute("DROP TABLE IF EXISTS items")
        cur.execute(f"""
            CREATE TABLE items (
                id SERIAL PRIMARY KEY,
                text TEXT,
                metadata JSONB,
                embedding VECTOR({dim})
            )
        """)
        print("Table 'items' created.")

        # Create Index
        cur.execute("CREATE INDEX ON items USING hnsw (embedding vector_cosine_ops)")
        print("Index created.")
        cur.close()

    def insert_data(self, data):
        if not self.conn:
            self.connect()

        # Ensure vector is registered if not already (e.g. if connected but setup not called in this session)
        try:
            register_vector(self.conn)
        except Exception:
            pass

        cur = self.conn.cursor()
        print(f"Inserting {len(data)} items...")

        for item in data:
            cur.execute(
                "INSERT INTO items (id, text, metadata, embedding) VALUES (%s, %s, %s, %s)",
                (item["id"], item["text"], json.dumps(item["metadata"]), item["vector"])
            )
        print("Data inserted.")
        cur.close()

    def search(self, query_vector, limit=5):
        if not self.conn:
            self.connect()

        try:
            register_vector(self.conn)
        except Exception:
            pass

        cur = self.conn.cursor()
        # Cosine distance: <=>
        cur.execute(f"""
            SELECT id, text, metadata, 1 - (embedding <=> %s) as similarity
            FROM items
            ORDER BY embedding <=> %s
            LIMIT {limit}
        """, (query_vector, query_vector))

        results = []
        for row in cur.fetchall():
            results.append({
                "id": row[0],
                "text": row[1],
                "metadata": row[2],
                "similarity": row[3]
            })
        cur.close()
        return results

    def delete_data(self, item_id):
        if not self.conn:
            self.connect()

        cur = self.conn.cursor()
        cur.execute("DELETE FROM items WHERE id = %s", (item_id,))
        cur.close()

    def teardown(self):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute("DROP TABLE IF EXISTS items")
            cur.close()
            self.conn.close()
