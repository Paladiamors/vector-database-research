# Vector Database Examples

This directory contains examples for embedding, storing, searching, and managing metadata in various self-hosted vector databases using Python.

## Prerequisites

1.  **Python 3.8+**
2.  **Docker & Docker Compose** (for running the databases)
3.  **Python Dependencies**: Install via `pip install -r requirements.txt`

## Dataset

A synthetic dataset is generated using `sentence-transformers`.
Run the generator script first:
```bash
python data/generate_data.py
```
This creates `data/dataset.json`.

## Examples

Each subdirectory contains a `docker-compose.yaml` (where applicable) and a `main.py` script.

To run an example (e.g., Weaviate):

1.  Navigate to the directory:
    ```bash
    cd weaviate
    ```
2.  Start the database:
    ```bash
    docker compose up -d
    ```
3.  Run the Python script:
    ```bash
    python main.py
    ```
4.  Stop the database:
    ```bash
    docker compose down
    ```

### Supported Databases

1.  **Weaviate** (`weaviate/`)
2.  **Milvus** (`milvus/`)
3.  **Qdrant** (`qdrant/`)
4.  **Chroma** (`chroma/`)
5.  **Deep Lake** (`deeplake/`) - *Library mode (no Docker needed)*
6.  **Faiss** (`faiss/`) - *Library mode (no Docker needed)*
7.  **PostgreSQL (pgvector)** (`postgres/`)
8.  **Redis** (`redis/`)
9.  **Elasticsearch** (`elasticsearch/`)
10. **OpenSearch** (`opensearch/`)
11. **Cassandra** (`cassandra/`)
12. **MariaDB** (`mariadb/`)
13. **Vespa** (`vespa/`)
14. **Vald** (`vald/`)
15. **ClickHouse** (`clickhouse/`)

## Notes

-   **Deep Lake** and **Faiss** run locally as libraries and do not require Docker containers.
-   **Vespa** example assumes deploying via HTTP; for production, use `vespa-cli` or `VespaDocker`.
-   **Vald** example uses a standalone agent configuration.
-   **MariaDB** example uses version 11.4+ with Vector support.
-   Ensure Docker has sufficient memory (at least 4GB recommended for some services like Milvus or Vespa).
