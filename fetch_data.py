# -*- coding: utf-8 -*-
"""
PostgreSQL data fetcher for the robotization funnel analysis pipeline.

Edit the CONFIG section below to match your database.
Everything else (the fetch_records function) should work without changes.
"""

import sys
import io
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG — change everything in this block to match your database
# ═══════════════════════════════════════════════════════════════════════════════

DB_HOST     = "localhost"       # Postgres host (e.g. "192.168.1.10")
DB_PORT     = 5432              # Postgres port (default: 5432)
DB_NAME     = "grafana"         # Database name
DB_USER     = "postgres"        # Database user
DB_PASSWORD = "password"        # Database password

# SQL query that returns one row per call.
# Must return ALL columns referenced in COLUMN_MAP below.
# Add WHERE / date filters here if you want to limit the data.
SQL_QUERY = """
SELECT
    call_id,
    call_date,
    language,
    autocompletion,
    stage_reached,
    transcription
FROM calls
-- WHERE call_date >= '2024-01-01'
ORDER BY call_date
"""

# Map from your actual DB column names → the field names the pipeline expects.
# Left side  = column name as returned by your SQL query (case-sensitive).
# Right side = DO NOT change — these are used by analyze_stages.py.
COLUMN_MAP = {
    "call_id":       "call_id",        # unique call identifier (str)
    "call_date":     "date",           # call date, will be formatted as YYYY-MM-DD (date/str)
    "language":      "language",       # "Russian" or "Kazakh"
    "autocompletion":"autocompletion", # True/False — did the robot complete the task?
    "stage_reached": "stage_reached",  # int 1-10 — funnel stage the call reached
    "transcription": "transcription",  # full call transcription text (str)
}

# ═══════════════════════════════════════════════════════════════════════════════
# END OF CONFIG
# ═══════════════════════════════════════════════════════════════════════════════


def fetch_records() -> list[dict]:
    """
    Connect to Postgres, run SQL_QUERY, and return a list of call records
    in the same format as generate_data.generate_dataset().

    Each record dict has keys: call_id, date, language, autocompletion,
    stage_reached, transcription.
    """
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        sys.exit(
            "ERROR: psycopg2 is not installed.\n"
            "Run: pip install psycopg2-binary"
        )

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SQL_QUERY)
            rows = cur.fetchall()
    finally:
        conn.close()

    records = []
    for row in rows:
        record = {}
        for db_col, pipeline_field in COLUMN_MAP.items():
            value = row[db_col]

            # Normalise date → "YYYY-MM-DD" string
            if pipeline_field == "date":
                value = str(value)[:10]

            # Normalise autocompletion → Python bool
            if pipeline_field == "autocompletion":
                value = bool(value)

            # Normalise stage_reached → int
            if pipeline_field == "stage_reached":
                value = int(value)

            record[pipeline_field] = value

        records.append(record)

    return records


if __name__ == "__main__":
    print("Fetching records from Postgres...")
    records = fetch_records()
    print(f"Fetched {len(records)} records.")
    if records:
        print("Sample record:")
        sample = records[0]
        for k, v in sample.items():
            preview = str(v)[:80] + "..." if len(str(v)) > 80 else str(v)
            print(f"  {k}: {preview}")
