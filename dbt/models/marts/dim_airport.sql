{{
    config(
        materialized='table',
        schema='public',
    )
}}

WITH airports AS (
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY airport_name) as airport_id,
        airport_name,
        country_name,
        airport_continent as continent,
        airport_country_code as airport_code
    FROM {{ source('raw', 'raw_airline') }}
    WHERE airport_name IS NOT NULL
)

SELECT * FROM airports
