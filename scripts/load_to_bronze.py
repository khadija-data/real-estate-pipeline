import os
import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Charger le CSV
df = pd.read_csv("data/real_estate_raw.csv")

# Connexion à Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

# Créer la table Bronze
cursor.execute("""
CREATE TABLE IF NOT EXISTS REAL_ESTATE_BRONZE (
    listing_id       STRING,
    property_type    STRING,
    country          STRING,
    city             STRING,
    neighborhood      STRING,
    surface_m2        FLOAT,
    num_rooms         INT,
    num_bathrooms     INT,
    floor             INT,
    year_built        INT,
    price             STRING,
    listing_date      STRING,
    "condition"       STRING,
    heating_type      STRING,
    parking           STRING,
    energy_rating     STRING,
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
""")

# Colonnes dans l'ordre attendu par l'INSERT
columns = [
    "listing_id", "property_type", "country", "city", "neighborhood",
    "surface_m2", "num_rooms", "num_bathrooms", "floor", "year_built",
    "price", "listing_date", "condition", "heating_type", "parking",
    "energy_rating"
]

def clean(val):
    """Convertit NaN -> None et les types numpy -> types Python natifs."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return None if np.isnan(val) else float(val)
    if isinstance(val, str) and val.strip().lower() in ("nan", "none", ""):
        return None
    return val

# Préparer les données proprement (évite le bug NAN de Snowflake)
rows = [
    tuple(clean(v) for v in row)
    for row in df[columns].itertuples(index=False, name=None)
]

# Insérer les données
cursor.executemany(f"""
INSERT INTO REAL_ESTATE_BRONZE (
    {", ".join(columns)}
)
VALUES (
    {", ".join(["%s"] * len(columns))}
)
""", rows)

# Sauvegarder
conn.commit()

print(f" {len(rows)} lignes insérées dans REAL_ESTATE_BRONZE.")

# Fermer la connexion
cursor.close()
conn.close()

print(" Bronze layer loaded successfully!")