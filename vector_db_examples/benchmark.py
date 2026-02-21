import os
import sys
import json
import time
import subprocess
import argparse
import importlib.util
import statistics

# List of supported databases (folder names)
DATABASES = [
    "clickhouse", "elasticsearch", "mariadb", "milvus",
    "opensearch", "postgres", "qdrant", "redis",
    "vald", "vespa", "weaviate"
]

def load_dataset(path):
    with open(path, 'r') as f:
        return json.load(f)

def run_command(command, cwd=None):
    try:
        subprocess.run(command, check=True, shell=True, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False

def get_db_module(db_name):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    lib_path = os.path.join(script_dir, db_name, "lib.py")
    if not os.path.exists(lib_path):
        return None

    spec = importlib.util.spec_from_file_location(f"{db_name}_lib", lib_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"{db_name}_lib"] = module
    spec.loader.exec_module(module)
    return module

def get_db_class(db_name, module):
    # Mapping folder name to ClassName
    mapping = {
        "clickhouse": "ClickHouseDB",
        "elasticsearch": "ElasticsearchDB",
        "mariadb": "MariaDB",
        "milvus": "MilvusDB",
        "opensearch": "OpenSearchDB",
        "postgres": "PostgresDB",
        "qdrant": "QdrantDB",
        "redis": "RedisDB",
        "vald": "ValdDB",
        "vespa": "VespaDB",
        "weaviate": "WeaviateDB"
    }
    class_name = mapping.get(db_name)
    if hasattr(module, class_name):
        return getattr(module, class_name)
    return None

def benchmark_db(db_name, data, args):
    print(f"Benchmarking {db_name}...")
    metrics = {"database": db_name, "status": "failed"}

    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_dir = os.path.join(script_dir, db_name)

    # 1. Start Docker
    compose_file = os.path.join(db_dir, "docker-compose.yaml")
    if not os.path.exists(compose_file):
        compose_file = os.path.join(db_dir, "docker-compose.yml")

    if not os.path.exists(compose_file):
        print(f"No docker-compose file found for {db_name}, skipping.")
        metrics["error"] = "no_docker_compose"
        return metrics

    print(f"Starting {db_name} via docker-compose...")
    if not run_command("docker-compose up -d", cwd=db_dir):
        print("Failed to start docker-compose.")
        metrics["error"] = "docker_start_failed"
        return metrics

    try:
        # 2. Load Lib
        module = get_db_module(db_name)
        if not module:
            print("Library not found.")
            metrics["error"] = "lib_not_found"
            return metrics

        DBClass = get_db_class(db_name, module)
        if not DBClass:
            print("DB Class not found.")
            metrics["error"] = "class_not_found"
            return metrics

        # 3. Instantiate and Connect
        try:
            db = DBClass()
        except ImportError as e:
            print(f"Dependency missing: {e}")
            metrics["error"] = "dependency_missing"
            return metrics
        except Exception as e:
            print(f"Init failed: {e}")
            metrics["error"] = "init_failed"
            return metrics

        # Wait for ready
        print("Waiting for DB to be ready...")
        ready = False
        for _ in range(30): # Wait up to 30*2 = 60s (or more depending on sleep)
            if db.check_ready():
                ready = True
                break
            time.sleep(2)

        if not ready:
            print("DB not ready in time.")
            metrics["error"] = "timeout"
            return metrics

        # 4. Setup
        print("Setup...")
        start_time = time.time()
        db.setup(len(data[0]["vector"]))
        metrics["setup_time"] = time.time() - start_time

        # 5. Insert
        print("Inserting...")
        start_time = time.time()
        db.insert_data(data)
        metrics["insert_time"] = time.time() - start_time
        metrics["items_count"] = len(data)

        # 6. Search
        print("Searching...")
        query_vector = data[0]["vector"]
        latencies = []
        for _ in range(10): # Run 10 times
            t0 = time.time()
            db.search(query_vector, limit=5)
            latencies.append(time.time() - t0)

        metrics["search_avg_latency"] = statistics.mean(latencies)
        metrics["search_p99_latency"] = sorted(latencies)[int(len(latencies)*0.99)]

        # 7. Teardown
        print("Teardown...")
        db.teardown()

        metrics["status"] = "success"
        print(f"Finished {db_name}.")

    except Exception as e:
        print(f"Error during benchmark: {e}")
        metrics["error"] = str(e)
    finally:
        # Stop Docker
        print("Stopping docker...")
        run_command("docker-compose down", cwd=db_dir)

    return metrics

def main():
    parser = argparse.ArgumentParser(description="Vector DB Benchmark")
    parser.add_argument("--db", type=str, default="all", help="Comma separated list of DBs to run or 'all'")
    parser.add_argument("--data", type=str, default="data/dataset_large.json", help="Path to dataset")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_dir, args.data)

    # Generate data if not exists and using default
    if args.data == "data/dataset_large.json" and not os.path.exists(data_path):
        print("Generating data...")
        from generate_data import generate_data
        generate_data()

    if not os.path.exists(data_path):
        print(f"Data file {data_path} not found.")
        return

    data = load_dataset(data_path)

    if args.db == "all":
        dbs_to_run = DATABASES
    else:
        dbs_to_run = args.db.split(",")

    all_metrics = []

    for db_name in dbs_to_run:
        db_name = db_name.strip()
        if db_name not in DATABASES:
            print(f"Unknown DB: {db_name}")
            continue

        m = benchmark_db(db_name, data, args)
        all_metrics.append(m)

    # Output
    output_file = os.path.join(script_dir, "metrics.json")
    with open(output_file, 'w') as f:
        json.dump(all_metrics, f, indent=2)

    print(f"Metrics saved to {output_file}")

if __name__ == "__main__":
    main()
