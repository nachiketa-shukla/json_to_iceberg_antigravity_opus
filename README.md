# json-to-iceberg

Load nested JSON from SaaS API responses into Apache Iceberg tables.

## Features

- **Recursive flattening** of deeply nested JSON into flat, dot-delimited columns
- **Auto-detection** of the records array (`[{…}]`, `{"data": [{…}]}`, or single object)
- **Null-type resolution** — columns that are all-`null` are defaulted to `String`
- **Schema registration** with an Iceberg REST catalog
- **Data writing** to S3-compatible object stores via PyIceberg + PyArrow

## Dependencies

| Package | Purpose |
|---|---|
| `polars` | DataFrame operations |
| `pyarrow` | Polars ↔ Iceberg bridge |
| `pyiceberg[s3fs]` | Iceberg catalog + S3 I/O |

## Quick start

```bash
# Install
pip install -e .

# Set environment variables (or use defaults for local docker-compose)
export ICEBERG_REST_URI=http://localhost:8181
export ICEBERG_WAREHOUSE=s3://warehouse/
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY=admin
export S3_SECRET_KEY=password

# Start local infrastructure
docker compose up -d

# Ingest a JSON file
json-to-iceberg ingest \
  --input fixtures/sample_api_response.json \
  --namespace default \
  --table users \
  --mode overwrite \
  --verbose
```

## Configuration

All settings are read from environment variables:

| Variable | Default | Description |
|---|---|---|
| `ICEBERG_REST_URI` | `http://localhost:8181` | REST catalog endpoint |
| `ICEBERG_WAREHOUSE` | `s3://warehouse/` | Warehouse path |
| `S3_ENDPOINT` | `http://localhost:9000` | S3-compatible endpoint |
| `S3_ACCESS_KEY` | `admin` | S3 access key |
| `S3_SECRET_KEY` | `password` | S3 secret key |
| `S3_REGION` | `us-east-1` | S3 region |
| `ICEBERG_NAMESPACE` | `default` | Target namespace |
| `ICEBERG_TABLE` | `raw` | Target table name |

## Development

```bash
pip install -e ".[dev]"
pytest -v
```

## Architecture

```
JSON file
  │
  ▼
flatten.py ── recursive flatten → Polars DataFrame
  │
  ▼
schema.py ── resolve Null types → cast to Arrow Table
  │
  ▼
writer.py ── REST catalog → create/load table → write to S3
```
