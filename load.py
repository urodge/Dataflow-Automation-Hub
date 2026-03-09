"""
load.py — Step 3 of the Weather ETL Pipeline
Loads the clean DataFrame into a SQLite database.
"""

import sqlite3
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

DB_PATH = "weather.db"


def load(df: pd.DataFrame = None) -> None:
    """
    Inserts clean weather data into the SQLite 'weather' table.
    Creates the table if it doesn't exist.
    """
    if df is None:
        df = pd.read_csv("data/clean_weather.csv")

    log.info(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # ── Create table schema if it doesn't exist ────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                city        TEXT    NOT NULL,
                temp_c      REAL,
                humidity    INTEGER,
                wind_kmph   REAL,
                description TEXT,
                feels_hot   BOOLEAN,
                wind_strong BOOLEAN,
                loaded_at   DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

        # ── Add a loaded_at timestamp column ───────────────────
        df["loaded_at"] = datetime.utcnow().isoformat()

        # ── Write DataFrame rows into the table ────────────────
        df.to_sql("weather", conn, if_exists="append", index=False)

        log.info(f"Load complete → {len(df)} rows inserted into '{DB_PATH}'")

    except Exception as e:
        log.error(f"Load failed: {e}")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    load()
