"""Load processed Airbnb Seattle CSV files into PostgreSQL Data Warehouse."""

from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

import os


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
SCHEMA_FILE = PROJECT_ROOT / "sql" / "schema.sql"
DB_SCHEMA = "airbnb_seattle"

LOAD_ORDER: Dict[str, List[str]] = {
    "dim_date": [
        "date_id",
        "date",
        "year",
        "quarter",
        "month",
        "month_name",
        "day",
        "day_of_week",
        "day_name",
        "week_of_year",
        "is_weekend",
    ],
    "dim_room_type": ["room_type_id", "room_type"],
    "dim_location": [
        "location_id",
        "neighbourhood_cleansed",
        "neighbourhood_group_cleansed",
        "city",
        "state",
        "zipcode",
        "country",
        "latitude",
        "longitude",
    ],
    "dim_listing": [
        "listing_id",
        "listing_name",
        "host_id",
        "host_name",
        "property_type",
        "room_type_id",
        "location_id",
        "accommodates",
        "bathrooms",
        "bedrooms",
        "beds",
        "bed_type",
        "price",
        "minimum_nights",
        "maximum_nights",
        "number_of_reviews",
        "actual_review_count",
        "first_review_date",
        "last_review_date",
        "review_scores_rating",
        "reviews_per_month",
    ],
    "fact_availability": [
        "listing_id",
        "date_id",
        "available_flag",
        "booked_flag",
        "price",
        "estimated_revenue",
    ],
}


def get_database_url() -> URL:
    load_dotenv(PROJECT_ROOT / ".env")

    required_vars = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        missing = ", ".join(missing_vars)
        raise ValueError(f"Missing database configuration in .env: {missing}")

    return URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        database=os.environ["DB_NAME"],
    )


def run_schema_sql(engine) -> None:
    schema_sql = SCHEMA_FILE.read_text(encoding="utf-8")
    raw_connection = engine.raw_connection()
    try:
        with raw_connection.cursor() as cursor:
            cursor.execute(schema_sql)
        raw_connection.commit()
    except Exception:
        raw_connection.rollback()
        raise
    finally:
        raw_connection.close()


def validate_processed_files() -> None:
    missing_files = [
        str(PROCESSED_DIR / f"{table_name}.csv")
        for table_name in LOAD_ORDER
        if not (PROCESSED_DIR / f"{table_name}.csv").exists()
    ]
    if missing_files:
        missing = "\n".join(missing_files)
        raise FileNotFoundError(f"Missing processed CSV files:\n{missing}")


def copy_csv_to_table(engine, table_name: str, columns: List[str]) -> int:
    csv_path = PROCESSED_DIR / f"{table_name}.csv"
    column_list = ", ".join(columns)
    copy_sql = (
        f"COPY {DB_SCHEMA}.{table_name} ({column_list}) "
        "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    )

    raw_connection = engine.raw_connection()
    try:
        with raw_connection.cursor() as cursor:
            with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
                cursor.copy_expert(copy_sql, csv_file)
            cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{table_name}")
            row_count = cursor.fetchone()[0]
        raw_connection.commit()
    except Exception:
        raw_connection.rollback()
        raise
    finally:
        raw_connection.close()

    return row_count


def load_processed_data(engine) -> None:
    validate_processed_files()

    print("\n=== LOAD CSV TO POSTGRESQL ===")
    for table_name, columns in LOAD_ORDER.items():
        row_count = copy_csv_to_table(engine, table_name, columns)
        print(f"- {table_name}: {row_count:,} rows loaded")


def main() -> None:
    engine = create_engine(get_database_url())

    print("Recreating PostgreSQL Data Warehouse tables...")
    run_schema_sql(engine)
    load_processed_data(engine)
    print("\nLoad completed successfully.")


if __name__ == "__main__":
    main()
