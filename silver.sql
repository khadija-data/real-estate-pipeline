{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM bronze.real_estate_raw

),

CAST(price AS FLOAT) AS price,

TO_DATE(listing_date) AS listing_date,

CAST(year_built AS INT) AS year_built,

CAST(num_rooms AS INT) AS num_rooms,

CAST(num_bathrooms AS INT) AS num_bathrooms,

CAST(floor AS INT) AS floor