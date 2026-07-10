{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{ source('bronze', 'real_estate_bronze') }}

),

cleaned AS (

    SELECT
        TRY_CAST(listing_id AS INT) AS listing_id,

        CASE
            WHEN LOWER(TRIM(property_type)) IN ('apt', 'apartment') THEN 'apartment'
            WHEN LOWER(TRIM(property_type)) = 'villa' THEN 'villa'
            WHEN LOWER(TRIM(property_type)) = 'house' THEN 'house'
            WHEN LOWER(TRIM(property_type)) = 'studio' THEN 'studio'
            WHEN LOWER(TRIM(property_type)) = 'duplex' THEN 'duplex'
            WHEN LOWER(TRIM(property_type)) = 'penthouse' THEN 'penthouse'
            ELSE NULLIF(LOWER(TRIM(property_type)), '')
        END AS property_type,

        NULLIF(TRIM(country), '') AS country,
        NULLIF(TRIM(city), '') AS city,

        CASE
            WHEN LOWER(TRIM(neighborhood)) IN ('suburbs', 'suburb') THEN 'Suburbs'
            WHEN LOWER(TRIM(neighborhood)) IN ('city center', 'city centre', 'center', 'centre') THEN 'City Center'
            WHEN LOWER(TRIM(neighborhood)) = 'historic' THEN 'Historic'
            WHEN LOWER(TRIM(neighborhood)) = 'industrial' THEN 'Industrial'
            WHEN LOWER(TRIM(neighborhood)) = 'residential' THEN 'Residential'
            ELSE NULLIF(INITCAP(TRIM(neighborhood)), '')
        END AS neighborhood,

        CASE
            WHEN TRY_CAST(surface_m2 AS FLOAT) BETWEEN 15 AND 1000
                THEN TRY_CAST(surface_m2 AS FLOAT)
            ELSE NULL
        END AS surface_m2,

        TRY_CAST(num_rooms AS INT) AS num_rooms,
        TRY_CAST(num_bathrooms AS INT) AS num_bathrooms,
        TRY_CAST(floor AS INT) AS floor,

        CASE
            WHEN TRY_CAST(year_built AS INT) BETWEEN 1800 AND DATE_PART('year', CURRENT_DATE())
                THEN TRY_CAST(year_built AS INT)
            ELSE NULL
        END AS year_built,

        CASE
            WHEN TRY_CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9.]', ''), '') AS FLOAT)
                 BETWEEN 5000 AND 5000000
                THEN TRY_CAST(NULLIF(REGEXP_REPLACE(price, '[^0-9.]', ''), '') AS FLOAT)
            ELSE NULL
        END AS price,

        COALESCE(
            TRY_TO_DATE(listing_date, 'YYYY-MM-DD'),
            TRY_TO_DATE(listing_date, 'DD/MM/YYYY'),
            TRY_TO_DATE(listing_date, 'DD.MM.YYYY'),
            TRY_TO_DATE(listing_date, 'MM-DD-YYYY')
        ) AS listing_date,

        NULLIF(INITCAP(TRIM(heating_type)), '') AS heating_type,

        CASE
            WHEN UPPER(TRIM(parking)) IN ('YES', 'Y', 'TRUE', '1') THEN TRUE
            WHEN UPPER(TRIM(parking)) IN ('NO', 'N', 'FALSE', '0') THEN FALSE
            ELSE NULL
        END AS parking,

        NULLIF(UPPER(TRIM(energy_rating)), '') AS energy_rating,

        load_timestamp AS bronze_loaded_at

    FROM base

),

dedup AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY listing_id
            ORDER BY bronze_loaded_at DESC
        ) AS rn
    FROM cleaned
    WHERE listing_id IS NOT NULL

),

latest AS (

    SELECT *
    FROM dedup
    WHERE rn = 1

),

numeric_stats AS (

    SELECT
        MEDIAN(surface_m2) AS median_surface_m2,
        MEDIAN(num_rooms) AS median_num_rooms,
        MEDIAN(num_bathrooms) AS median_num_bathrooms,
        MEDIAN(floor) AS median_floor,
        MEDIAN(year_built) AS median_year_built,
        MEDIAN(price) AS median_price
    FROM latest

),

mode_property_type AS (

    SELECT property_type
    FROM latest
    WHERE property_type IS NOT NULL
    GROUP BY property_type
    ORDER BY COUNT(*) DESC, property_type
    LIMIT 1

),

mode_country AS (

    SELECT country
    FROM latest
    WHERE country IS NOT NULL
    GROUP BY country
    ORDER BY COUNT(*) DESC, country
    LIMIT 1

),

mode_city AS (

    SELECT city
    FROM latest
    WHERE city IS NOT NULL
    GROUP BY city
    ORDER BY COUNT(*) DESC, city
    LIMIT 1

),

mode_neighborhood AS (

    SELECT neighborhood
    FROM latest
    WHERE neighborhood IS NOT NULL
    GROUP BY neighborhood
    ORDER BY COUNT(*) DESC, neighborhood
    LIMIT 1

),

mode_heating_type AS (

    SELECT heating_type
    FROM latest
    WHERE heating_type IS NOT NULL
    GROUP BY heating_type
    ORDER BY COUNT(*) DESC, heating_type
    LIMIT 1

),

mode_parking AS (

    SELECT parking
    FROM latest
    WHERE parking IS NOT NULL
    GROUP BY parking
    ORDER BY COUNT(*) DESC, parking
    LIMIT 1

),

mode_energy_rating AS (

    SELECT energy_rating
    FROM latest
    WHERE energy_rating IS NOT NULL
    GROUP BY energy_rating
    ORDER BY COUNT(*) DESC, energy_rating
    LIMIT 1

),

mode_listing_date AS (

    SELECT listing_date
    FROM latest
    WHERE listing_date IS NOT NULL
    GROUP BY listing_date
    ORDER BY COUNT(*) DESC, listing_date
    LIMIT 1

)

SELECT
    l.listing_id,

    COALESCE(l.property_type, mpt.property_type) AS property_type,
    COALESCE(l.country, mc.country) AS country,
    COALESCE(l.city, mci.city) AS city,
    COALESCE(l.neighborhood, mn.neighborhood) AS neighborhood,

    COALESCE(l.surface_m2, ns.median_surface_m2) AS surface_m2,
    COALESCE(l.num_rooms, ns.median_num_rooms) AS num_rooms,
    COALESCE(l.num_bathrooms, ns.median_num_bathrooms) AS num_bathrooms,
    COALESCE(l.floor, ns.median_floor) AS floor,
    COALESCE(l.year_built, ns.median_year_built) AS year_built,
    COALESCE(l.price, ns.median_price) AS price,

    COALESCE(l.listing_date, mld.listing_date) AS listing_date,
    COALESCE(l.heating_type, mht.heating_type) AS heating_type,
    COALESCE(l.parking, mp.parking) AS parking,
    COALESCE(l.energy_rating, mer.energy_rating) AS energy_rating,

    ROUND(
        COALESCE(l.price, ns.median_price)
        / NULLIF(COALESCE(l.surface_m2, ns.median_surface_m2), 0),
        2
    ) AS price_per_m2,

    DATE_PART('year', CURRENT_DATE()) - COALESCE(l.year_built, ns.median_year_built) AS property_age,

    COALESCE(l.bronze_loaded_at, CURRENT_TIMESTAMP()) AS bronze_loaded_at

FROM latest l
CROSS JOIN numeric_stats ns
CROSS JOIN mode_property_type mpt
CROSS JOIN mode_country mc
CROSS JOIN mode_city mci
CROSS JOIN mode_neighborhood mn
CROSS JOIN mode_heating_type mht
CROSS JOIN mode_parking mp
CROSS JOIN mode_energy_rating mer
CROSS JOIN mode_listing_date mld