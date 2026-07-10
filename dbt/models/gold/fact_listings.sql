{{ config(materialized='table', schema='GOLD') }}

WITH
loc AS (SELECT * FROM {{ ref('dim_location') }}),
pr  AS (SELECT * FROM {{ ref('dim_property') }}),
dt  AS (SELECT * FROM {{ ref('dim_date') }})

SELECT
    ROW_NUMBER() OVER (ORDER BY s.listing_id) AS listing_sk,
    s.listing_id,
    loc.location_sk,
    pr.property_sk,
    dt.date_sk,
    s.price,
    s.surface_m2,
    s.price_per_m2,
    s.num_rooms,
    s.num_bathrooms,
    s.floor,
    s.property_age
FROM {{ ref('stg_listings') }} s
LEFT JOIN loc
    ON  s.country      = loc.country
    AND s.city         = loc.city
    AND COALESCE(s.neighborhood, '') = COALESCE(loc.neighborhood, '')
LEFT JOIN pr
    ON  s.property_type = pr.property_type
    AND s.heating_type  = pr.heating_type
    AND s.energy_rating = pr.energy_rating
    AND s.parking       = pr.parking
LEFT JOIN dt
    ON s.listing_date = dt.full_date