{{ config(materialized='table', schema='GOLD') }}

SELECT
    ROW_NUMBER() OVER (
        ORDER BY country, city, neighborhood
    )               AS location_sk,
    country,
    city,
    neighborhood
FROM (
    SELECT DISTINCT
        country,
        city,
        neighborhood
    FROM {{ ref('stg_listings') }}
    WHERE country IS NOT NULL
      AND city    IS NOT NULL
)