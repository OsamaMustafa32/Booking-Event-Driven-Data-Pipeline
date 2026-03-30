-- Analytics on bookings.daily_bookings_fact (after Glue loads).

-- How many rows and how status breaks down
SELECT COUNT(*) AS total_fact_rows
FROM bookings.daily_bookings_fact;

SELECT
    booking_status,
    COUNT(*) AS row_count
FROM bookings.daily_bookings_fact
GROUP BY booking_status
ORDER BY row_count DESC;

-- Only confirmed rows (good for revenue and “how many bookings” when you want stable money)
SELECT
    COUNT(DISTINCT booking_id) AS confirmed_distinct_bookings,
    COALESCE(SUM(total_amount), 0) AS confirmed_total_revenue,
    COALESCE(AVG(total_amount), 0) AS confirmed_average_booking_value,
    COALESCE(AVG(DATEDIFF(day, check_in_date, check_out_date)), 0) AS confirmed_average_stay_nights
FROM bookings.daily_bookings_fact
WHERE UPPER(TRIM(booking_status)) = 'CONFIRMED';

-- Share of rows that look cancelled (includes lifecycle cancel rows, not only “new” cancels)
SELECT
    ROUND(
        100.0 * SUM(
            CASE
                WHEN UPPER(TRIM(booking_status)) IN ('CANCELLED', 'CANCELED') THEN 1
                ELSE 0
            END
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS percent_rows_marked_cancelled
FROM bookings.daily_bookings_fact;

-- Bookings and revenue by calendar day of booking_timestamp
SELECT
    CAST(booking_timestamp AS DATE) AS booking_calendar_date,
    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(booking_status)) = 'CONFIRMED' THEN booking_id
    END) AS confirmed_distinct_bookings,
    COUNT(*) AS all_rows_in_table_for_that_date
FROM bookings.daily_bookings_fact
GROUP BY CAST(booking_timestamp AS DATE)
ORDER BY booking_calendar_date;

SELECT
    CAST(booking_timestamp AS DATE) AS booking_calendar_date,
    ROUND(
        SUM(
            CASE
                WHEN UPPER(TRIM(booking_status)) = 'CONFIRMED' THEN total_amount
                ELSE 0
            END
        ),
        2
    ) AS confirmed_revenue_for_date
FROM bookings.daily_bookings_fact
GROUP BY CAST(booking_timestamp AS DATE)
ORDER BY booking_calendar_date;

-- Top cities by confirmed revenue
SELECT
    city,
    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(booking_status)) = 'CONFIRMED' THEN booking_id
    END) AS confirmed_distinct_bookings,
    ROUND(
        SUM(
            CASE
                WHEN UPPER(TRIM(booking_status)) = 'CONFIRMED' THEN total_amount
                ELSE 0
            END
        ),
        2
    ) AS confirmed_total_revenue
FROM bookings.daily_bookings_fact
GROUP BY city
ORDER BY confirmed_total_revenue DESC NULLS LAST
LIMIT 15;

-- Channel mix
SELECT
    channel_name,
    channel_type,
    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(booking_status)) = 'CONFIRMED' THEN booking_id
    END) AS confirmed_distinct_bookings,
    ROUND(
        SUM(
            CASE
                WHEN UPPER(TRIM(booking_status)) = 'CONFIRMED' THEN total_amount
                ELSE 0
            END
        ),
        2
    ) AS confirmed_total_revenue
FROM bookings.daily_bookings_fact
GROUP BY channel_name, channel_type
ORDER BY confirmed_total_revenue DESC NULLS LAST;

-- Cancelled row rate by channel (all rows in the slice)
SELECT
    channel_name,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN UPPER(TRIM(booking_status)) IN ('CANCELLED', 'CANCELED') THEN 1
                ELSE 0
            END
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS percent_rows_cancelled,
    COUNT(*) AS row_count_all_statuses
FROM bookings.daily_bookings_fact
GROUP BY channel_name
ORDER BY percent_rows_cancelled DESC NULLS LAST;

-- Property type and star rating (confirmed revenue)
SELECT
    property_type,
    star_rating,
    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(booking_status)) = 'CONFIRMED' THEN booking_id
    END) AS confirmed_distinct_bookings,
    ROUND(
        SUM(
            CASE
                WHEN UPPER(TRIM(booking_status)) = 'CONFIRMED' THEN total_amount
                ELSE 0
            END
        ),
        2
    ) AS confirmed_total_revenue
FROM bookings.daily_bookings_fact
GROUP BY property_type, star_rating
ORDER BY confirmed_total_revenue DESC NULLS LAST;

-- Stay length by room type (confirmed only)
SELECT
    room_type,
    ROUND(AVG(DATEDIFF(day, check_in_date, check_out_date)), 2) AS average_stay_nights,
    COUNT(DISTINCT booking_id) AS distinct_booking_count
FROM bookings.daily_bookings_fact
WHERE UPPER(TRIM(booking_status)) = 'CONFIRMED'
GROUP BY room_type
ORDER BY average_stay_nights DESC NULLS LAST;

-- Lead time: days from booking date to check-in (confirmed only)
WITH lead_time AS (
    SELECT
        DATEDIFF(day, CAST(booking_timestamp AS DATE), check_in_date) AS days_before_checkin
    FROM bookings.daily_bookings_fact
    WHERE UPPER(TRIM(booking_status)) = 'CONFIRMED'
)
SELECT
    CASE
        WHEN days_before_checkin < 0 THEN 'checkin_before_booking_date'
        WHEN days_before_checkin <= 7 THEN '0_to_7_days'
        WHEN days_before_checkin <= 30 THEN '8_to_30_days'
        WHEN days_before_checkin <= 90 THEN '31_to_90_days'
        ELSE 'over_90_days'
    END AS lead_time_bucket,
    COUNT(*) AS confirmed_booking_rows
FROM lead_time
GROUP BY
    CASE
        WHEN days_before_checkin < 0 THEN 'checkin_before_booking_date'
        WHEN days_before_checkin <= 7 THEN '0_to_7_days'
        WHEN days_before_checkin <= 30 THEN '8_to_30_days'
        WHEN days_before_checkin <= 90 THEN '31_to_90_days'
        ELSE 'over_90_days'
    END
ORDER BY
    MIN(
        CASE
            WHEN days_before_checkin < 0 THEN 5
            WHEN days_before_checkin <= 7 THEN 1
            WHEN days_before_checkin <= 30 THEN 2
            WHEN days_before_checkin <= 90 THEN 3
            ELSE 4
        END
    );

-- Payment method share of confirmed revenue
SELECT
    payment_method,
    ROUND(SUM(total_amount), 2) AS confirmed_total_revenue,
    ROUND(
        100.0 * SUM(total_amount)
        / NULLIF(SUM(SUM(total_amount)) OVER (), 0),
        2
    ) AS percent_of_confirmed_revenue
FROM bookings.daily_bookings_fact
WHERE UPPER(TRIM(booking_status)) = 'CONFIRMED'
GROUP BY payment_method
ORDER BY confirmed_total_revenue DESC NULLS LAST;

-- Same booking_id more than once (cross-day or same-day lifecycle / duplicates in the fact)
SELECT
    booking_id,
    booking_status,
    CAST(booking_timestamp AS DATE) AS event_calendar_date,
    total_amount
FROM bookings.daily_bookings_fact
WHERE booking_id IN (
    SELECT booking_id
    FROM bookings.daily_bookings_fact
    GROUP BY booking_id
    HAVING COUNT(*) > 1
)
ORDER BY booking_id, booking_timestamp
LIMIT 50;
