"""ETL pipeline for Airbnb Seattle Business Intelligence project.

Reads raw Airbnb CSV files, cleans key fields, builds a simple star schema,
and exports processed CSV files for PostgreSQL or Power BI.
"""

from pathlib import Path
from typing import Tuple

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def clean_price(series: pd.Series) -> pd.Series:
    """Convert Airbnb currency strings like '$1,234.00' to float."""
    return pd.to_numeric(
        series.astype("string").str.replace(r"[$,]", "", regex=True),
        errors="coerce",
    )


def build_dim_date(calendar_df: pd.DataFrame) -> pd.DataFrame:
    dates = (
        calendar_df[["date"]]
        .dropna()
        .drop_duplicates()
        .sort_values("date")
        .reset_index(drop=True)
    )

    dates["date_id"] = dates["date"].dt.strftime("%Y%m%d").astype(int)
    dates["year"] = dates["date"].dt.year
    dates["quarter"] = dates["date"].dt.quarter
    dates["month"] = dates["date"].dt.month
    dates["month_name"] = dates["date"].dt.month_name()
    dates["day"] = dates["date"].dt.day
    dates["day_of_week"] = dates["date"].dt.dayofweek + 1
    dates["day_name"] = dates["date"].dt.day_name()
    dates["week_of_year"] = dates["date"].dt.isocalendar().week.astype(int)
    dates["is_weekend"] = dates["date"].dt.dayofweek.isin([5, 6]).astype(int)

    return dates[
        [
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
        ]
    ]


def build_dim_room_type(listings_df: pd.DataFrame) -> pd.DataFrame:
    room_types = (
        listings_df[["room_type"]]
        .drop_duplicates()
        .sort_values("room_type")
        .reset_index(drop=True)
    )
    room_types.insert(0, "room_type_id", room_types.index + 1)
    return room_types


def build_dim_location(listings_df: pd.DataFrame) -> pd.DataFrame:
    location_cols = [
        "neighbourhood_cleansed",
        "neighbourhood_group_cleansed",
        "city",
        "state",
        "zipcode",
        "country",
        "latitude",
        "longitude",
    ]
    locations = (
        listings_df[location_cols]
        .drop_duplicates()
        .sort_values(["neighbourhood_group_cleansed", "neighbourhood_cleansed", "zipcode"])
        .reset_index(drop=True)
    )
    locations.insert(0, "location_id", locations.index + 1)
    return locations


def build_dim_listing(
    listings_df: pd.DataFrame,
    reviews_df: pd.DataFrame,
    dim_room_type: pd.DataFrame,
    dim_location: pd.DataFrame,
) -> pd.DataFrame:
    review_summary = (
        reviews_df.groupby("listing_id", as_index=False)
        .agg(
            actual_review_count=("id", "count"),
            first_review_date=("date", "min"),
            last_review_date=("date", "max"),
        )
    )

    location_cols = [
        "neighbourhood_cleansed",
        "neighbourhood_group_cleansed",
        "city",
        "state",
        "zipcode",
        "country",
        "latitude",
        "longitude",
    ]

    listing_cols = [
        "id",
        "name",
        "host_id",
        "host_name",
        "property_type",
        "room_type",
        "accommodates",
        "bathrooms",
        "bedrooms",
        "beds",
        "bed_type",
        "price",
        "minimum_nights",
        "maximum_nights",
        "number_of_reviews",
        "review_scores_rating",
        "reviews_per_month",
    ] + location_cols

    dim_listing = listings_df[listing_cols].copy()
    dim_listing = dim_listing.merge(dim_room_type, on="room_type", how="left")
    dim_listing = dim_listing.merge(dim_location, on=location_cols, how="left")
    dim_listing = dim_listing.merge(
        review_summary,
        left_on="id",
        right_on="listing_id",
        how="left",
    )

    dim_listing["actual_review_count"] = dim_listing["actual_review_count"].fillna(0).astype(int)
    dim_listing = dim_listing.drop(columns=["listing_id"])
    dim_listing = dim_listing.rename(columns={"id": "listing_id", "name": "listing_name"})

    return dim_listing[
        [
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
        ]
    ]


def build_fact_availability(calendar_df: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    fact = calendar_df[["listing_id", "date", "available", "price"]].copy()
    fact = fact.merge(dim_date[["date_id", "date"]], on="date", how="left")

    fact["available_flag"] = fact["available"].eq("t").astype(int)
    fact["booked_flag"] = fact["available"].eq("f").astype(int)
    fact["estimated_revenue"] = fact["booked_flag"] * fact["price"]

    return fact[
        [
            "listing_id",
            "date_id",
            "available_flag",
            "booked_flag",
            "price",
            "estimated_revenue",
        ]
    ]


def load_raw_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    listings = pd.read_csv(RAW_DIR / "listings.csv", low_memory=False)
    calendar = pd.read_csv(RAW_DIR / "calendar.csv", low_memory=False)
    reviews = pd.read_csv(RAW_DIR / "reviews.csv", low_memory=False)
    return listings, calendar, reviews


def clean_data(
    listings: pd.DataFrame,
    calendar: pd.DataFrame,
    reviews: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    listings = listings.copy()
    calendar = calendar.copy()
    reviews = reviews.copy()

    listings["price"] = clean_price(listings["price"])
    calendar["price"] = clean_price(calendar["price"])
    calendar["date"] = pd.to_datetime(calendar["date"], errors="coerce")
    reviews["date"] = pd.to_datetime(reviews["date"], errors="coerce")

    # Use listing-level price when a calendar row has no price, then fall back to
    # the median price to keep fact rows usable for BI aggregation.
    listing_price = listings.set_index("id")["price"]
    calendar["price"] = calendar["price"].fillna(calendar["listing_id"].map(listing_price))
    calendar["price"] = calendar["price"].fillna(calendar["price"].median())

    listings["price"] = listings["price"].fillna(calendar["price"].median())
    listings["name"] = listings["name"].fillna("Unknown listing")
    listings["host_name"] = listings["host_name"].fillna("Unknown host")

    text_defaults = {
        "property_type": "Unknown",
        "room_type": "Unknown",
        "bed_type": "Unknown",
        "neighbourhood_cleansed": "Unknown",
        "neighbourhood_group_cleansed": "Unknown",
        "city": "Seattle",
        "state": "WA",
        "zipcode": "Unknown",
        "country": "United States",
    }
    for column, default_value in text_defaults.items():
        listings[column] = listings[column].fillna(default_value)

    numeric_defaults = {
        "host_id": 0,
        "accommodates": 0,
        "bathrooms": listings["bathrooms"].median(),
        "bedrooms": listings["bedrooms"].median(),
        "beds": listings["beds"].median(),
        "minimum_nights": 0,
        "maximum_nights": 0,
        "number_of_reviews": 0,
        "review_scores_rating": 0,
        "reviews_per_month": 0,
        "latitude": listings["latitude"].median(),
        "longitude": listings["longitude"].median(),
    }
    for column, default_value in numeric_defaults.items():
        listings[column] = listings[column].fillna(default_value)

    calendar["available"] = calendar["available"].fillna("f")

    return listings, calendar, reviews


def print_data_quality_report(
    dim_date: pd.DataFrame,
    dim_listing: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_room_type: pd.DataFrame,
    fact_availability: pd.DataFrame,
) -> None:
    tables = {
        "dim_date": dim_date,
        "dim_listing": dim_listing,
        "dim_location": dim_location,
        "dim_room_type": dim_room_type,
        "fact_availability": fact_availability,
    }

    print("\n=== DATA QUALITY REPORT ===")
    print("\nRow counts:")
    for table_name, df in tables.items():
        print(f"- {table_name}: {len(df):,}")

    important_nulls = {
        "dim_date": ["date_id", "date"],
        "dim_listing": ["listing_id", "listing_name", "room_type_id", "location_id", "price"],
        "dim_location": ["location_id", "neighbourhood_cleansed", "city"],
        "dim_room_type": ["room_type_id", "room_type"],
        "fact_availability": ["listing_id", "date_id", "available_flag", "booked_flag", "price"],
    }

    print("\nImportant null counts:")
    for table_name, columns in important_nulls.items():
        null_counts = tables[table_name][columns].isna().sum()
        print(f"- {table_name}: {null_counts.to_dict()}")

    print("\nDate range:")
    print(f"- min date: {dim_date['date'].min().date()}")
    print(f"- max date: {dim_date['date'].max().date()}")

    print("\nCalendar price statistics:")
    print(f"- min price: {fact_availability['price'].min():,.2f}")
    print(f"- max price: {fact_availability['price'].max():,.2f}")
    print(f"- avg price: {fact_availability['price'].mean():,.2f}")


def export_outputs(
    dim_date: pd.DataFrame,
    dim_listing: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_room_type: pd.DataFrame,
    fact_availability: pd.DataFrame,
) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    dim_date.to_csv(PROCESSED_DIR / "dim_date.csv", index=False)
    dim_listing.to_csv(PROCESSED_DIR / "dim_listing.csv", index=False)
    dim_location.to_csv(PROCESSED_DIR / "dim_location.csv", index=False)
    dim_room_type.to_csv(PROCESSED_DIR / "dim_room_type.csv", index=False)
    fact_availability.to_csv(PROCESSED_DIR / "fact_availability.csv", index=False)


def main() -> None:
    listings, calendar, reviews = load_raw_data()
    listings, calendar, reviews = clean_data(listings, calendar, reviews)

    dim_date = build_dim_date(calendar)
    dim_room_type = build_dim_room_type(listings)
    dim_location = build_dim_location(listings)
    dim_listing = build_dim_listing(listings, reviews, dim_room_type, dim_location)
    fact_availability = build_fact_availability(calendar, dim_date)

    export_outputs(dim_date, dim_listing, dim_location, dim_room_type, fact_availability)
    print_data_quality_report(
        dim_date,
        dim_listing,
        dim_location,
        dim_room_type,
        fact_availability,
    )
    print(f"\nProcessed files exported to: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
