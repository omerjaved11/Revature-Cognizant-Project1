# ETL Web Application

A lightweight ETL and data exploration web app built with FastAPI and pandas. The application lets users upload CSV files as data sources, preview and validate them, apply simple cleaning steps, export and replay a transformation pipeline, and load curated datasets into PostgreSQL. It also includes a Retail ETL demo flow.

This app is designed for local development and classroom or portfolio use, with a clean path toward production hardening.

---

## Key Features

### Data Sources

* Upload CSV files with optional skip-rows support.
* Auto-saves source metadata to the database.
* Stores raw files under `data/input` and/or a dedicated sources folder used by the app.
* In-browser preview of the first 10 rows.
* Validation report per column:

  * dtype
  * null count
  * null percentage
  * sample values
* Cleaning actions:

  * drop rows with null values
  * drop selected columns
* Save cleaned state back to disk.
* Export pipeline config as JSON.
* Replay pipeline from raw CSV.
* Load a source into PostgreSQL with modes:

  * overwrite
  * append

### Database Tables

* List user tables in the target schema.
* Preview table head in the UI.
* JSON API endpoint to fetch table rows.
* Basic visualizations for numeric and categorical columns.

### Retail ETL

* A demo ETL flow to showcase end-to-end extraction, transformation, and loading patterns for a retail dataset.

---

## Tech Stack

* **Backend:** FastAPI
* **Data Processing:** pandas
* **Database:** PostgreSQL
* **Templating:** Jinja2
* **UI Interactions:** HTMX (partials-based)
* **Containerization:** Docker / Docker Compose
* **Testing:** pytest

---

## Project Structure

```
.
├── config/
├── data/
│   └── input/
├── docker/
├── logs/
├── src/
│   ├── config/
│   ├── etl/
│   │   └── retail/
│   ├── logs/
│   ├── utils/
│   └── web/
├── static/
│   └── css/
├── templates/
│   └── partials/
└── tests/
```

### Directory Purpose

* **config/**
  Environment-level configuration files.

* **data/input/**
  Local data storage for uploaded or sample datasets.

* **docker/**
  Docker and Compose-related assets.

* **logs/**
  Top-level log output directory.

* **src/config/**
  Application configuration code (settings, constants, env loading).

* **src/etl/retail/**
  Retail ETL implementation and orchestration entrypoints.

* **src/logs/**
  App-level logging utilities or log configuration.

* **src/utils/**
  Shared utilities:

  * database helpers
  * pipeline helpers
  * logging
  * data load utilities

* **src/web/**
  FastAPI routes and web layer logic.

* **static/css/**
  Stylesheets.

* **templates/**
  Jinja2 templates.

* **templates/partials/**
  HTMX fragments for dynamic UI updates.

* **tests/**
  Unit and integration tests.

---

## Local Setup (Without Docker)

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the project root:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=postgres
DB_PASSWORD=postgres

# Optional
APP_ENV=development
LOG_LEVEL=INFO
```

### 4. Start PostgreSQL

Ensure your database is running and accessible based on the variables above.

### 5. Run the app

```bash
uvicorn src.web.app:app --reload
```

If your app entrypoint is different, replace this path with your actual module.

---

## Docker Setup

If you have a Compose setup in `docker/`:

```bash
docker compose up --build
```

To rebuild from scratch:

```bash
docker compose down -v
docker compose up --build
```

---

## Using the Web App

### Home

* `GET /`

### Sources

* `GET /sources`
* Upload CSV via UI
* Open, validate, clean, save, export config, replay pipeline

### Load to DB

* Use the UI load form or endpoint:

  * `POST /sources/{source_id}/load`

### Tables

* `GET /tables`
* Preview:

  * `GET /tables/{table_name}/preview`
* JSON API:

  * `GET /api/tables/{table_name}?limit=100`
* Visualize:

  * `GET /tables/{table_name}/visualize`

### Retail ETL

* `GET /etls`
* Run:

  * `POST /etls/retail/run`

---

## Testing

Run all tests:

```bash
pytest -q
```

If you have DB-dependent tests, ensure the test database is available and configured.

---

## Logging

Logs are written to:

* `logs/` (project-level)
* and/or `src/logs/` depending on your logger configuration.

You can control verbosity via:

```env
LOG_LEVEL=DEBUG
```

---

## Notes on Current Design

* The app maintains an in-memory DataFrame store for active sources to enable fast previews and step-by-step cleaning.
* Source files are also persisted to disk so sessions can be restored.
* Pipeline steps are stored in-memory during interaction and can be exported as JSON for reproducibility.

---

## Roadmap (Post Freeze)

Ideas to improve after the code freeze:

* Split large route modules into feature-based routers.
* Add a service layer for CSV handling, pipeline processing, and DB operations.
* Add stronger schema validation for uploads.
* Add batch implementation to load data in batches.
* Add Scheduler service to schedule the ETLs.
* Improve error handling with global exception handlers.
* Add authentication if the app is used in multi-user contexts.

---

## License

Omer 

---