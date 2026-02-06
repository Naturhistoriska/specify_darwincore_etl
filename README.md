# Specify Darwin Core ETL Pipeline

This project implements an Extract, Transform, Load (ETL) pipeline to process Darwin Core (DwC) archive data from a Specify database. It downloads a zipped DwC archive, extracts its contents, parses the Metadata using the [dwcahandler](https://github.com/AtlasOfLivingAustralia/dwcahandler) library, transforms the data using Dask, and loads the processed data into delimited text files.

## Features

*   **Download**: Fetches a zipped Darwin Core archive from a specified URL.
*   **Extract**: Robustly unzips the archive contents.
*   **Metadata Parsing**: Leverages the `dwcahandler` library to precisely parse `meta.xml`. This handles Darwin Core terms, XML namespaces, and complex core/extension relationships.
*   **Default Column Handling**: Automatically incorporates default values from `meta.xml`. If a column is defined in the metadata with a default value but is **missing from the data file**, it is explicitly added to the output. This ensures all expected fields are present for downstream processing.
*   **Transform**: Transforms extracted data using Dask DataFrames for memory-efficient processing of large datasets. This includes header mapping, data type handling, and deduplication.
*   **Load**: Saves the transformed data into new delimited text files. The format is configurable, defaulting to tab-separated (.txt) files compatible with GBIF IPT.
*   **Modular Architecture**: Clean separation between extraction, transformation, and loading.
*   **Production-Grade Logging**: Centralized logging in both text and JSON formats for better observability.
*   **Type Safety**: Full Mypy strict-mode compliance for high runtime reliability.

*   **Configurable**: Uses YAML configuration files (validated via Pydantic) for easy control.
*   **Dockerized**: Automated builds and publication to GHCR.

## Project Structure

```
.
├── config/           # Directory for YAML configuration files
├── etl/
│   ├── extract.py         # Handles downloading, extraction, and DwCA metadata mapping
│   ├── load.py            # Handles saving transformed data
│   ├── transform.py       # Handles memory-efficient transformation using Dask
│   ├── patches.py         # Isolates third-party library (dwcahandler) patches
│   ├── logging_config.py  # Centralized logging configuration
│   └── config_schema.py   # Pydantic models for configuration validation
├── main.py                # Pipeline entry point (ETLPipeline class)
├── pyproject.toml         # Project metadata and dependencies
├── Dockerfile             # Docker build instructions
└── .dockerignore          # Files to exclude from Docker image
```

## Setup (Local)

To set up and run the project locally, follow these steps:

1.  **Clone the repository**:
    ```bash
    git clone <repository_url>
    cd specify_darwincore_etl
    ```

2.  **Create a Python virtual environment**:
    ```bash
    python3.12 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install dependencies**:
    This project uses `requirements.txt` for dependency management.
    ```bash
    pip install -r requirements.txt
    pip install .
    ```


## Configuration

The pipeline's behavior is controlled by YAML configuration files in the `config/` directory.

```yaml
zip_path: data/nrm-zoo-ent.zip
extract_dir: data/entomology/extracted
output_dir: data/entomology/output
url: http://entomology.nrm.se/static/depository/export_feed/nrm-zoo-ent.zip
output_separator: "\t"
output_extension: ".txt"
download_retries: 3
download_backoff_factor: 0.3
log_format: "json"
read_encoding: "utf-8"
write_encoding: "utf-8"
on_bad_lines: "warn"
deduplicate_columns: []
```

*   `zip_path`: Local path for the downloaded zip file.
*   `extract_dir`: Directory for extracted archive contents.
*   `output_dir`: Directory for transformed output files.
*   `url`: URL of the Darwin Core archive.
*   `output_separator`: Field separator for output files (default: `\t`).
*   `output_extension`: File extension for output files (default: `.txt`).
*   `deduplicate_columns`: Columns used for deduplication. If empty (`[]`), all columns are used.

## Usage (Local)

To run the ETL pipeline locally:

1.  Ensure your configuration file in `config/` is correctly set up.
2.  Activate your virtual environment (e.g., `source .venv/bin/activate`).
3.  Run the `main.py` script:

    ```bash
    python main.py config/entomology-config.yml
    ```

    Specify a logging level if needed:
    ```bash
    python main.py config/entomology-config.yml --log-level INFO
    ```

## Docker Deployment

### 1. Build the Docker Image

```bash
docker build -t specify-darwincore-etl .
```

### 2. Run the Docker Container

Mount your local `config`, `data`, and `output` directories:

```bash
docker run \
  --rm \
  -v "$(pwd)/config:/app/config" \
  -v "$(pwd)/data:/app/data" \
  specify-darwincore-etl python main.py config/entomology-config.yml
```

### 3. Run with Docker Compose

For more streamlined local development and execution, you can use `docker-compose`. The `docker-compose.yml` file in the project root defines the service.

To build the service image:
```bash
docker-compose build
```

To run the ETL pipeline, specifying your configuration file:
```bash
docker-compose run --rm specify_darwincore_etl python main.py config/entomology-config.yml
```
You can also specify a logging level:
```bash
docker-compose run --rm specify_darwincore_etl python main.py config/entomology-config.yml --log-level INFO
```
