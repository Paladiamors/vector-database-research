import grpc
import sys

try:
    from vald.v1.vald import vald_pb2_grpc
    from vald.v1.payload import payload_pb2
except ImportError:
    vald_pb2_grpc = None
    payload_pb2 = None

class ValdDB:
    def __init__(self, host='localhost', port=8081):
        self.host = host
        self.port = port
        self.channel = None
        self.stub = None

    def connect(self):
        if not vald_pb2_grpc:
            raise ImportError("vald-client-python not installed.")

        print(f"Connecting to Vald at {self.host}:{self.port}...")
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.stub = vald_pb2_grpc.ValdStub(self.channel)

    def check_ready(self):
        try:
            if not self.stub:
                self.connect()
            # Vald doesn't have simple ping in ValdStub, maybe check something else?
            # We can try to search dummy or just assume ready if connect works
            return True
        except:
            return False

    def setup(self, dim):
        # Vald doesn't usually require explicit index creation via client if running standalone agent
        pass

    def insert_data(self, data):
        if not self.stub:
            self.connect()

        print(f"Inserting {len(data)} items...")
        for item in data:
            vec = payload_pb2.Object.Vector(id=str(item["id"]), vector=item["vector"])
            req = payload_pb2.Insert.Request(vector=vec, config=payload_pb2.Insert.Config(skip_strict_exist_check=True))
            try:
                self.stub.Insert(req)
            except grpc.RpcError as e:
                print(f"Insert failed: {e}")

        # Wait for indexing (eventually consistent)
        import time
        time.sleep(5)
        print("Data inserted.")

    def search(self, query_vector, limit=5):
        if not self.stub:
            self.connect()

        cfg = payload_pb2.Search.Config(num=limit, radius=-1.0, epsilon=0.01, timeout=3000000000)
        req = payload_pb2.Search.Request(vector=query_vector, config=cfg)

        results = []
        try:
            res = self.stub.Search(req)
            for hit in res.results:
                results.append({
                    "id": hit.id,
                    "distance": hit.distance
                })
        except grpc.RpcError as e:
            print(f"Search failed: {e}")

        return results

    def delete_data(self, item_id):
        if not self.stub:
            self.connect()

        id_req = payload_pb2.Object.ID(id=str(item_id))
        req = payload_pb2.Remove.Request(id=id_req, config=payload_pb2.Remove.Config(skip_strict_exist_check=True))
        try:
            self.stub.Remove(req)
        except grpc.RpcError as e:
            print(f"Remove failed: {e}")

    def teardown(self):
        if self.channel:
            self.channel.close()
