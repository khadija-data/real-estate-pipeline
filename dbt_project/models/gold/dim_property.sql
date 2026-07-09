{{ config(materialized='table', schema='GOLD') }}

SELECT
    ROW_NUMBER() OVER (
        ORDER BY property_type, energy_rating
    )               AS property_sk,
    property_type,
    heating_type,
    energy_rating,
    parking
FROM (
    SELECT DISTINCT
        property_type,
        heating_type,
        energy_rating,
        parking
    FROM {{ ref('stg_listings') }}
    WHERE property_type IS NOT NULL
)