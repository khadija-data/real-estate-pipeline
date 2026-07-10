{{ config(materialized='table', schema='GOLD') }}

SELECT
    ROW_NUMBER() OVER (
        ORDER BY listing_date
    )                             AS date_sk,
    listing_date::DATE            AS full_date,
    YEAR(listing_date::DATE)      AS year,
    QUARTER(listing_date::DATE)   AS quarter,
    MONTH(listing_date::DATE)     AS month,
    MONTHNAME(listing_date::DATE) AS month_name
FROM (
    SELECT DISTINCT listing_date
    FROM {{ ref('stg_listings') }}
    WHERE listing_date IS NOT NULL
)