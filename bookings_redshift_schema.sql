-- Redshift setup for the booking pipeline (schema, dimension tables, fact shell).
-- Enter YOUR_ACCOUNT_ID in the IAM_ROLE lines.

CREATE SCHEMA bookings;

-- Hotels / properties (loaded once from S3; Glue reads this via the data catalog)
CREATE TABLE bookings.properties_dim (
    property_id    BIGINT,
    property_name  VARCHAR(200),
    property_type  VARCHAR(50),
    star_rating    INT,
    chain_name     VARCHAR(100),
    city           VARCHAR(100),
    country        VARCHAR(100),
    country_code   VARCHAR(5)
);

COPY bookings.properties_dim
FROM 's3://booking-edp-data/dims/properties_dim.csv'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/AmazonRedshift-CommandsAccessRole-Booking-EDP'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';

-- Booking channels (small reference table, same pattern)
CREATE TABLE bookings.booking_channels_dim (
    channel_id    BIGINT,
    channel_name  VARCHAR(100),
    channel_type  VARCHAR(50)
);

COPY bookings.booking_channels_dim
FROM 's3://booking-edp-data/dims/booking_channels_dim.csv'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/AmazonRedshift-CommandsAccessRole-Booking-EDP'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';

-- Fact table: one row per booking event after Glue (joins + data quality).
-- Glue can CREATE IF NOT EXISTS on load.
CREATE TABLE bookings.daily_bookings_fact (
    booking_id        VARCHAR(20),
    guest_id          BIGINT,
    property_name     VARCHAR(200),
    property_type     VARCHAR(50),
    star_rating       INT,
    chain_name        VARCHAR(100),
    city              VARCHAR(100),
    country           VARCHAR(100),
    country_code      VARCHAR(5),
    channel_name      VARCHAR(100),
    channel_type      VARCHAR(50),
    booking_status    VARCHAR(20),
    num_rooms         INT,
    num_adults        INT,
    num_children      INT,
    room_type         VARCHAR(50),
    check_in_date     DATE,
    check_out_date    DATE,
    total_amount      DECIMAL(10,2),
    currency          VARCHAR(5),
    payment_method    VARCHAR(30),
    booking_timestamp TIMESTAMP
);
