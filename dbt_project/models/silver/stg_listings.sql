{{ config(materialized='table', schema='SILVER') }}

SELECT
    listing_id,
    property_type,
    country,
    city,
    neighborhood,
    surface_m2,
    num_rooms,
    num_bathrooms,
    floor,
    year_built,
    price,
    listing_date,
    heating_type,
    parking,
    energy_rating,
    price_per_m2,
    property_age
FROM REAL_ESTATE_DB.BRONZE_SILVER.silver_real_estate