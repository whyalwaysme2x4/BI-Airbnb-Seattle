CREATE SCHEMA IF NOT EXISTS airbnb_seattle;

SET search_path TO airbnb_seattle;

DROP TABLE IF EXISTS fact_availability;
DROP TABLE IF EXISTS dim_listing;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_room_type;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year SMALLINT NOT NULL,
    is_weekend SMALLINT NOT NULL
);

COMMENT ON TABLE dim_date IS 'Dimension table for calendar dates used to analyze availability and revenue by day, week, month, quarter, and year.';
COMMENT ON COLUMN dim_date.date_id IS 'Primary key in YYYYMMDD format.';

CREATE TABLE dim_room_type (
    room_type_id INTEGER PRIMARY KEY,
    room_type VARCHAR(100) NOT NULL
);

COMMENT ON TABLE dim_room_type IS 'Dimension table for Airbnb room type categories such as entire home, private room, and shared room.';
COMMENT ON COLUMN dim_room_type.room_type_id IS 'Surrogate primary key for room type.';

CREATE TABLE dim_location (
    location_id INTEGER PRIMARY KEY,
    neighbourhood_cleansed VARCHAR(150),
    neighbourhood_group_cleansed VARCHAR(150),
    city VARCHAR(100),
    state VARCHAR(50),
    zipcode VARCHAR(30),
    country VARCHAR(100),
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6)
);

COMMENT ON TABLE dim_location IS 'Dimension table for Seattle listing geography including neighbourhood, zipcode, and coordinates.';
COMMENT ON COLUMN dim_location.location_id IS 'Surrogate primary key for listing location.';

CREATE TABLE dim_listing (
    listing_id BIGINT PRIMARY KEY,
    listing_name TEXT,
    host_id BIGINT,
    host_name VARCHAR(255),
    property_type VARCHAR(100),
    room_type_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    accommodates SMALLINT,
    bathrooms NUMERIC(5, 2),
    bedrooms NUMERIC(5, 2),
    beds NUMERIC(5, 2),
    bed_type VARCHAR(100),
    price NUMERIC(12, 2),
    minimum_nights INTEGER,
    maximum_nights INTEGER,
    number_of_reviews INTEGER,
    actual_review_count INTEGER,
    first_review_date DATE,
    last_review_date DATE,
    review_scores_rating NUMERIC(5, 2),
    reviews_per_month NUMERIC(8, 2),
    CONSTRAINT fk_dim_listing_room_type
        FOREIGN KEY (room_type_id) REFERENCES dim_room_type(room_type_id),
    CONSTRAINT fk_dim_listing_location
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);

COMMENT ON TABLE dim_listing IS 'Dimension table for Airbnb listings, hosts, property attributes, review summary, and links to room type and location dimensions.';
COMMENT ON COLUMN dim_listing.listing_id IS 'Natural primary key from Airbnb listing id.';

CREATE TABLE fact_availability (
    listing_id BIGINT NOT NULL,
    date_id INTEGER NOT NULL,
    available_flag SMALLINT NOT NULL,
    booked_flag SMALLINT NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    estimated_revenue NUMERIC(12, 2) NOT NULL,
    CONSTRAINT pk_fact_availability PRIMARY KEY (listing_id, date_id),
    CONSTRAINT fk_fact_availability_listing
        FOREIGN KEY (listing_id) REFERENCES dim_listing(listing_id),
    CONSTRAINT fk_fact_availability_date
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    CONSTRAINT chk_fact_availability_available_flag
        CHECK (available_flag IN (0, 1)),
    CONSTRAINT chk_fact_availability_booked_flag
        CHECK (booked_flag IN (0, 1)),
    CONSTRAINT chk_fact_availability_flag_logic
        CHECK (available_flag + booked_flag = 1)
);

COMMENT ON TABLE fact_availability IS 'Fact table at listing-day grain with availability, booked status, daily price, and estimated booked revenue.';
COMMENT ON COLUMN fact_availability.estimated_revenue IS 'Calculated as booked_flag multiplied by price.';

CREATE INDEX idx_fact_availability_date_id ON fact_availability(date_id);
CREATE INDEX idx_fact_availability_listing_id ON fact_availability(listing_id);
CREATE INDEX idx_dim_listing_room_type_id ON dim_listing(room_type_id);
CREATE INDEX idx_dim_listing_location_id ON dim_listing(location_id);
