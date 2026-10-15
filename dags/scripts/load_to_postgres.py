import csv
import json
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql


DAGS_DIR = Path(__file__).resolve().parents[1]
REPORT_FILE = DAGS_DIR / "reports" / "data_quality_report.json"

TABLES = {
    "sales": {
        "table": "sales_cleaned",
        "columns": ["order_id", "product", "quantity", "price"],
    },
    "employees": {
        "table": "employees_cleaned",
        "columns": ["employee_id", "name", "department", "salary"],
    },
}


def load_to_postgres():
    # These defaults match the standard local Airflow Compose setup.
    # Override them with environment variables if your credentials differ.
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "etl_pipeline"),
        user=os.getenv("PGUSER", "airflow"),
        password=os.getenv("PGPASSWORD", "airflow"),
    )

    total_loaded = 0

    try:
        with open(REPORT_FILE, encoding="utf-8") as file:
            report = json.load(file)

        with conn:
            with conn.cursor() as cursor:
                for result in report["files"]:
                    if result["status"] not in (
                        "processed",
                        "processed_with_rejections",
                    ):
                        continue

                    dataset = result["dataset"]

                    if dataset not in TABLES:
                        raise ValueError(
                            f"No PostgreSQL table mapping for {dataset}"
                        )

                    settings = TABLES[dataset]
                    cleaned_file = Path(result["cleaned_file"])

                    if not cleaned_file.is_file():
                        raise FileNotFoundError(cleaned_file)

                    table = sql.Identifier(settings["table"])
                    columns = settings["columns"]

                    insert_query = sql.SQL(
                        "INSERT INTO {} ({}) VALUES ({})"
                    ).format(
                        table,
                        sql.SQL(", ").join(
                            sql.Identifier(c) for c in columns
                        ),
                        sql.SQL(", ").join(
                            sql.Placeholder() for _ in columns
                        ),
                    )

                    loaded_for_file = 0

                    with open(
                        cleaned_file, encoding="utf-8-sig", newline=""
                    ) as csv_file:
                        reader = csv.DictReader(csv_file)

                        for row in reader:
                            values = [
                                row.get(column) or None
                                for column in columns
                            ]
                            cursor.execute(insert_query, values)
                            loaded_for_file += 1

                    total_loaded += loaded_for_file

                    print(
                        f"Loaded {loaded_for_file} rows from "
                        f"{cleaned_file.name} into {settings['table']}"
                    )

        print(f"Total rows loaded: {total_loaded}")

    finally:
        conn.close()


if __name__ == "__main__":
    load_to_postgres()