import pandas as pd
import snowflake.connector
from datetime import datetime
import os

# ─── Configuration ───────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT", "IQTAGLT-IU51886"),
    "user":      os.getenv("SNOWFLAKE_USER",    "KHADIJA"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD", ""),
    "database":  "REAL_ESTATE_DB",
    "schema":    "BRONZE",
    "warehouse": "COMPUTE_WH",
    "role":      "ACCOUNTADMIN"
}

CSV_PATH = "data/real_estate_raw.csv"
TABLE    = "REAL_ESTATE_BRONZE"

# ─── Load CSV ────────────────────────────────────────────
def load_csv(path: str) -> pd.DataFrame:
    print(f"[INFO] Loading CSV from {path}...")
    df = pd.read_csv(path)
    df["_loaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[INFO] {len(df)} rows loaded.")
    return df

# ─── Connect to Snowflake ────────────────────────────────
def get_connection():
    print("[INFO] Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print("[INFO] Connected successfully.")
    return conn

# ─── Create Bronze Table ─────────────────────────────────
def create_table(cursor):
    print(f"[INFO] Creating table {TABLE} if not exists...")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS REAL_ESTATE_DB.BRONZE.{TABLE} (
            listing_id      VARCHAR,
            property_type   VARCHAR,
            country         VARCHAR,
            city            VARCHAR,
            neighborhood    VARCHAR,
            surface_m2      VARCHAR,
            num_rooms       VARCHAR,
            num_bathrooms   VARCHAR,
            floor           VARCHAR,
            year_built      VARCHAR,
            price           VARCHAR,
            listing_date    VARCHAR,
            condition       VARCHAR,
            heating_type    VARCHAR,
            parking         VARCHAR,
            energy_rating   VARCHAR,
            _loaded_at      VARCHAR
        )
    """)
    print("[INFO] Table ready.")

# ─── Insert Data ─────────────────────────────────────────
def insert_data(cursor, df: pd.DataFrame):
    print(f"[INFO] Inserting {len(df)} rows into Bronze...")
    df = df.where(pd.notnull(df), None)
    rows = [tuple(row) for row in df.itertuples(index=False)]
    cursor.executemany(
        f"""
        INSERT INTO REAL_ESTATE_DB.BRONZE.{TABLE} VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        rows
    )
    print(f"[INFO] {len(df)} rows inserted successfully.")

# ─── Main ────────────────────────────────────────────────
def main():
    df   = load_csv(CSV_PATH)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        create_table(cursor)
        insert_data(cursor, df)
        conn.commit()
        print("[SUCCESS] Bronze layer loaded.")
    except Exception as e:
        print(f"[ERROR] {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()