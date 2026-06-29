import pandas as pd
import snowflake.connector
from datetime import datetime

# 1. Load CSV
df = pd.read_csv("data/real_estate_raw.csv")

# 2. Add metadata column — store as string to avoid binding issues
df["load_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # ← Fix here

# 3. Connect to Snowflake
conn = snowflake.connector.connect(
    user="KHADIJA",
    password="Snowflake123//",
    account="IQTAGLT-IU51886",
    warehouse="COMPUTE_WH",
    database="REAL_ESTATE_DB",
    schema="BRONZE"
)

cursor = conn.cursor()

# 4. Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS REAL_ESTATE_BRONZE (
    listing_id STRING,
    property_type STRING,
    country STRING,
    city STRING,
    neighborhood STRING,
    surface_m2 FLOAT,
    num_rooms INT,
    num_bathrooms INT,
    floor INT,
    year_built INT,
    price STRING,
    listing_date STRING,
    condition STRING,
    heating_type STRING,
    parking STRING,
    energy_rating STRING,
    load_timestamp TIMESTAMP
)
""")

# 5. Insert data row by row — cast NaN to None for clean nulls
for _, row in df.iterrows():
    values = tuple(None if pd.isna(v) else v for v in row)  # ← Bonus fix
    cursor.execute("""
        INSERT INTO REAL_ESTATE_BRONZE VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, values)

conn.commit()
cursor.close()
conn.close()

print("Bronze layer loaded successfully!")