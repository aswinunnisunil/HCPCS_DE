# HCPCS Data Engineering Pipeline

An end-to-end data engineering project that extracts HCPCS (Healthcare
Common Procedure Coding System) data, transforms and validates records,
and loads them into PostgreSQL using Slowly Changing Dimension Type 2
(SCD Type 2). Apache Airflow orchestrates the pipeline, while Metabase
provides data and pipeline reporting.

## Project Overview

The pipeline collects HCPCS codes and descriptions from
[HCPCSData](https://www.hcpcsdata.com), stores raw and transformed
snapshots as JSON files, and maintains current and historical records in
PostgreSQL.

The project demonstrates: - Web data extraction and structured file
staging - Data cleaning, normalization, deduplication, and change-hash
generation - PostgreSQL warehousing with SCD Type 2 history - Airflow
orchestration - SQL-based data quality validation - Failure logging and
pipeline execution metrics - Metabase reporting - Unit testing, linting,
and GitHub Actions CI - Docker-based local services

### Current project snapshot

The latest recorded successful Airflow run processed 8,770 records: -
Extracted: 8,770 - Transformed: 8,770 - Inserted: 0 - Updated: 0 -
Unchanged: 8,770 - Duration: approximately 63.7 seconds

The unchanged result means the incoming records matched the current
warehouse records during that run.

## Architecture

``` text
HCPCSData website
       |
       v
Python extractor (Requests + BeautifulSoup)
       |
       v
Timestamped raw JSON files
       |
       v
Python transformation
(cleaning, normalization, deduplication, hash)
       |
       v
Timestamped processed JSON files
       |
       v
PostgreSQL warehouse (SCD Type 2)
       |
       +--------------------------+
       |                          |
       v                          v
Data quality checks        Pipeline metrics
       |                          |
       +-------------+------------+
                     |
                     v
              Metabase dashboard

Apache Airflow orchestrates extraction, transformation,
loading, validation, and metrics recording.
```

## Technology Stack

  Area              Technology
  ----------------- ---------------------------------
  Language          Python
  Extraction        Requests, BeautifulSoup
  Transformation    Python, Pandas
  Staging           JSON
  Database          PostgreSQL 18
  Database access   SQLAlchemy, psycopg2
  Orchestration     Apache Airflow 3.1.3
  Containers        Docker, Docker Compose
  Data quality      SQL and Python
  Monitoring        Python logging and Airflow logs
  Observability     PostgreSQL pipeline metrics
  BI                Metabase
  Testing           Pytest
  Linting           Flake8
  CI                GitHub Actions

## Pipeline Workflow

### 1. Extraction

`scrapper.py` retrieves the available HCPCS code categories and extracts
code, category, and description information.

-   Uses Requests and BeautifulSoup to retrieve and parse source pages.
-   Avoids duplicate codes during extraction.
-   Writes a timestamped JSON snapshot to `raw/`.

### 2. Transformation

`transform.py` processes the latest raw JSON snapshot.

-   Cleans and normalizes text and code fields.
-   Removes duplicate records.
-   Generates an MD5 hash from the description for change detection.
-   Writes a timestamped transformed JSON file to `data/processed/`.

### 3. Loading and SCD Type 2

`load.py` loads the latest transformed file into `public.hcpcs_codes`.

For each incoming HCPCS code, the loader compares its description hash
with the current warehouse record.

  -----------------------------------------------------------------------
  Scenario                            Warehouse action
  ----------------------------------- -----------------------------------
  Code does not exist                 Insert the first version

  Description hash is unchanged       Keep the current version unchanged

  Description hash has changed        Expire the current version and
                                      insert a new version
  -----------------------------------------------------------------------

SCD Type 2 tracking uses: - `valid_from`: when a version becomes
effective - `valid_to`: when a version is expired; `NULL` for the
current version - `is_current`: identifies the active version -
`version`: version number for the code

The previous version is retained when a description changes rather than
being overwritten.

### 4. Data Quality

`validation.py` runs seven database-level checks:

1.  Duplicate current HCPCS codes
2.  Codes without exactly one current version
3.  Missing codes or descriptions
4.  Invalid version numbers
5.  Invalid historical record dates
6.  Current records with an end date
7.  Historical records without an end date

The validation process reports each check as PASS or FAIL and raises an
error if any check fails.

### 5. Orchestration

The Airflow DAG is defined in `dags/hcpcs_pipeline_dag.py`.

It orchestrates: 1. Extraction 2. Transformation 3. Loading 4. Data
quality validation 5. Pipeline metrics recording

The metrics task uses an `ALL_DONE` trigger rule so it can record
execution information after upstream tasks finish, including
unsuccessful runs.

Airflow is exposed locally at `http://localhost:8081`.

### 6. Monitoring and Failure Handling

`monitoring.py` provides an Airflow failure callback that logs: - DAG
ID - Task ID - Run ID - Exception information

This supports investigation through Airflow logs. External email or
Slack notifications are not configured.

### 7. Observability

Execution metrics are stored in `public.pipeline_metrics`.

Tracked information includes: - DAG ID and run ID - Run status - Start
and end timestamps - Duration in seconds - Records extracted and
transformed - Records inserted, updated, and unchanged - Error
information

### 8. BI and Reporting

Metabase connects to PostgreSQL and provides the **HCPCS Data
Engineering Dashboard**, containing:

-   **HCPCS Codes by Category** --- distribution of active codes across
    categories
-   **Pipeline Execution Summary** --- run status, execution counts,
    average duration, and processing totals
-   **HCPCS Pipeline Run History** --- detailed execution history and
    record counts

Metabase is exposed locally at `http://localhost:3000`.

## Repository Structure

``` text
HCPCS/
├── .github/
│   └── workflows/
│       └── ci.yml
├── airflow/
│   ├── Dockerfile
│   └── requirements.txt
├── dags/
│   └── hcpcs_pipeline_dag.py
├── database/
│   └── schema.sql
├── data/
│   ├── raw/
│   └── processed/
├── raw/
├── tests/
│   ├── test_transform.py
│   ├── test_load.py
│   └── test_validation.py
├── scrapper.py
├── transform.py
├── load.py
├── validation.py
├── monitoring.py
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

Generated data and logs may be excluded from source control according to
`.gitignore`.

## Prerequisites

-   Python 3.12
-   PostgreSQL 18
-   Docker Desktop with WSL2
-   Git

## Local Setup

### 1. Clone the repository

``` bash
git clone https://github.com/aswinunnisunil/HCPCS_DE.git
cd HCPCS_DE
```

### 2. Create and activate a virtual environment

PowerShell:

``` powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

``` powershell
python -m pip install -r requirements-dev.txt
```

### 4. Configure database credentials

Create a `.env` file in the project root using `.env.example` as a
template:

``` dotenv
DB_USER=your_postgres_username
DB_PASSWORD=your_postgres_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
```

Use your own local PostgreSQL database values. Do not commit `.env` or
credentials to GitHub.

### 5. Create the warehouse table

Execute `database/schema.sql` against the PostgreSQL database configured
in `.env`. This creates the `public.hcpcs_codes` table and its
supporting indexes.

The pipeline also writes execution metrics to `public.pipeline_metrics`;
the metrics table is managed by the project's Airflow observability
implementation.

## Running the Python Modules Individually

Run commands from the project root, in this order:

``` powershell
python scrapper.py
python transform.py
python load.py
python validation.py
```

Each module uses the latest matching input file from its configured data
directory.

## Running Airflow and Metabase with Docker Compose

The project uses Docker Compose to run Airflow and Metabase.

Start the services:

``` powershell
docker compose up -d --build
```

Check service status:

``` powershell
docker compose ps
```

Open the interfaces:

  Service          Local URL
  ---------------- -----------------------
  Apache Airflow   http://localhost:8081
  Metabase         http://localhost:3000

The Compose configuration mounts the project and DAG directories into
the Airflow container. The Airflow container connects to the host
PostgreSQL instance using `host.docker.internal`.

## Testing and Code Quality

Run the unit tests:

``` powershell
python -m pytest tests/ -v
```

Run Flake8:

``` powershell
python -m flake8 --exclude=.venv,logs,.pytest_cache .
```

The local test suite contains 11 tests covering transformation, loading,
and validation logic.

## Continuous Integration

The GitHub Actions workflow is located at `.github/workflows/ci.yml`.

It is configured to run on pushes and pull requests targeting `main`.
The workflow: 1. Checks out the repository. 2. Sets up Python 3.12. 3.
Installs `requirements-dev.txt`. 4. Runs the Pytest suite. 5. Runs
Flake8.

The workflow has completed successfully on GitHub after the YAML
indentation issue was corrected.

## Future Enhancements

-   Add email or Slack notifications for pipeline failures.
-   Add integration tests against a dedicated test database.
-   Extend historical change analysis and reporting.
-   Add more detailed performance and operational metrics.
-   Add automated deployment and release workflows.

## Author

**Aswin Unni Sunil**

Data Engineering \| Python \| SQL \| PostgreSQL \| Apache Airflow \| ETL

GitHub: [aswinunnisunil](https://github.com/aswinunnisunil)
