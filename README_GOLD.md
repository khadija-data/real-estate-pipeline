# 🥇 Gold Layer — Star Schema

## Overview
Gold layer built with dbt on top of Silver, implementing a Star Schema for Power BI analytics.

## Schema
```
FACT_LISTINGS ──→ DIM_LOCATION (country, city, neighborhood)
              ──→ DIM_PROPERTY (type, heating, energy, parking)
              ──→ DIM_DATE     (year, quarter, month)
```

## Models
| Model | Rows | Description |
|-------|------|-------------|
| dim_location | 410 | Geographic dimension |
| dim_property | 325 | Property characteristics |
| dim_date | 1382 | Time dimension |
| fact_listings | 2000 | Main fact table |

## Run
```bash
dbt run --select gold
dbt test --select gold
```

## Tests
- 24/24 dbt tests PASS ✅
- unique, not_null, relationships

## Justification — Star Schema
- Simple et performant pour Power BI
- Adapté au volume de données (2000 lignes)
- Facilite les agrégations et filtres