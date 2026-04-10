## 🚀 Postgres Integration Guide

This update allows the pipeline to fetch real data directly from a PostgreSQL database. The `fetch_data.py` script replaces the synthetic generator while maintaining the same output format.

---

### 1. Installation
Install the new database dependencies:
```bash
pip install -r requirements.txt
```

### 2. Configuration
The only file you need to modify is `fetch_data.py`. Update the **CONFIG** block at the top of the script:

| Variable | Description |
| :--- | :--- |
| **DB_HOST** | Postgres host IP or hostname |
| **DB_PORT** | Port (usually `5432`) |
| **DB_NAME** | Database name |
| **DB_USER / DB_PASSWORD** | Database credentials |
| **SQL_QUERY** | Your custom query (set table name and `WHERE` filters) |
| **COLUMN_MAP** | Map your DB column names (left) to pipeline fields (right) |

> **Note:** The pipeline expects one row per call including: `call_id`, `date`, `language`, `completed` (boolean), `stage` (1–10), and `transcription`. If your schema differs, adjust the `SQL_QUERY` to join or compute these values.

---

### 3. Usage

#### Test Connection
Verify your database connection and query logic independently:
```bash
python fetch_data.py
```

#### Run the Analysis
Use the `--source` flag to toggle between production data and local JSON files:

```bash
# Fetch from Postgres (Production)
python analyze_stages.py --source postgres

# Use local JSON file (Default)
python analyze_stages.py --source json
```
