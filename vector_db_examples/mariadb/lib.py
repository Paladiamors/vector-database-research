import sys
import json

try:
    import mariadb
except ImportError:
    mariadb = None

class MariaDB:
    def __init__(self, host='127.0.0.1', port=3306, user='root', password='password', database='vectordb'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        if not mariadb:
            raise ImportError("mariadb package is required. Please install it.")

        print(f"Connecting to MariaDB at {self.host}:{self.port}...")
        try:
            self.conn = mariadb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=True
            )
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB: {e}")
            raise

    def check_ready(self):
        try:
            if not self.conn:
                self.connect()
            self.conn.ping()
            return True
        except:
            return False

    def setup(self, dim):
        if not self.conn:
            self.connect()

        cur = self.conn.cursor()
        try:
            cur.execute("DROP TABLE IF EXISTS items")
            # Create table with VECTOR type
            # VECTOR INDEX requires MariaDB 11.7+
            cur.execute(f"""
                CREATE TABLE items (
                    id INT PRIMARY KEY,
                    text TEXT,
                    category VARCHAR(100),
                    vector VECTOR({dim}) NOT NULL,
                    VECTOR INDEX (vector)
                ) ENGINE=InnoDB
            """)
            print("Table 'items' created with VECTOR type.")
        except mariadb.Error as e:
            print(f"Error creating table: {e}")
            raise
        finally:
            cur.close()

    def insert_data(self, data):
        if not self.conn:
            self.connect()

        cur = self.conn.cursor()
        print(f"Inserting {len(data)} items...")

        for item in data:
            # Convert vector to string representation for VEC_FromText
            vec_str = str(item["vector"])
            try:
                cur.execute(
                    "INSERT INTO items (id, text, category, vector) VALUES (?, ?, ?, VEC_FromText(?))",
                    (item["id"], item["text"], item["metadata"]["category"], vec_str)
                )
            except mariadb.Error as e:
                print(f"Error inserting item {item['id']}: {e}")

        print("Data inserted.")
        cur.close()

    def search(self, query_vector, limit=5):
        if not self.conn:
            self.connect()

        cur = self.conn.cursor()
        vec_str = str(query_vector)

        # Using VEC_DISTANCE_EUCLIDEAN
        cur.execute(f"""
            SELECT id, text, category, VEC_DISTANCE_EUCLIDEAN(vector, VEC_FromText(?)) as dist
            FROM items
            ORDER BY dist ASC
            LIMIT {limit}
        """, (vec_str,))

        results = []
        for row in cur:
            results.append(row)

        cur.close()
        return results

    def delete_data(self, item_id):
        if not self.conn:
            self.connect()

        cur = self.conn.cursor()
        cur.execute("DELETE FROM items WHERE id = ?", (item_id,))
        cur.close()

    def teardown(self):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute("DROP TABLE IF EXISTS items")
            cur.close()
            self.conn.close()
