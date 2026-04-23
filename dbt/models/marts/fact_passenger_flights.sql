{{
    config(
        materialized='table',
        schema='public',
    )
}}

WITH source_data AS (
    SELECT
        passenger_id,
        CAST(departure_date AS DATE) as flight_date,
        COALESCE(airport_name, 'Unknown') as departure_airport,
        COALESCE(arrival_airport, 'Unknown') as arrival_airport,
        COALESCE(flight_status, 'Unknown') as flight_status,
        age,
        gender,
        nationality,
        first_name,
        last_name,
        ingested_at,
        silver_processed_at
    FROM {{ source('raw', 'raw_airline') }}
    WHERE passenger_id IS NOT NULL
      AND departure_date IS NOT NULL
)

SELECT * FROM source_data
