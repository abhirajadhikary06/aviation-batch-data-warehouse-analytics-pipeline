{{
    config(
        materialized='table',
        schema='public',
    )
}}

WITH flight_data AS (
    SELECT
        CAST(departure_date AS DATE) as flight_date,
        flight_status,
        COUNT(*) as flight_count
    FROM {{ source('raw', 'raw_airline') }}
    WHERE departure_date IS NOT NULL
      AND flight_status IS NOT NULL
    GROUP BY 1, 2
),

metrics AS (
    SELECT
        flight_date,
        SUM(CASE WHEN flight_status = 'On Time' THEN flight_count ELSE 0 END) as on_time_count,
        SUM(CASE WHEN flight_status = 'Delayed' THEN flight_count ELSE 0 END) as delayed_count,
        SUM(CASE WHEN flight_status = 'Cancelled' THEN flight_count ELSE 0 END) as cancelled_count,
        SUM(flight_count) as total_flights,
        ROUND(
            100.0 * SUM(CASE WHEN flight_status = 'On Time' THEN flight_count ELSE 0 END)
            / NULLIF(SUM(flight_count), 0),
            2
        ) as on_time_percentage
    FROM flight_data
    GROUP BY 1
)

SELECT * FROM metrics
