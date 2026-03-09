"""
transform.py — Step 2 of the Weather ETL Pipeline
Reads raw CSV, cleans data using Pandas, and saves a clean version.
"""

import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def transform(input_file: str = "data/raw_weather.csv") -> pd.DataFrame:
    """
    Cleans and reshapes raw weather data.
    Returns a clean Pandas DataFrame.
    """
    log.info(f"Loading raw data from {input_file}")
    df = pd.read_csv(input_file)
    original_count = len(df)

    # ── 1. Drop rows with any missing values ──────────────────
    df = df.dropna()
    log.info(f"Dropped {original_count - len(df)} rows with null values")

    # ── 2. Convert columns to correct data types ──────────────
    df["temp_c"]     = df["temp_c"].astype(float)
    df["humidity"]   = df["humidity"].astype(int)
    df["wind_kmph"]  = df["wind_kmph"].astype(float)

    # ── 3. Filter out physically impossible values ─────────────
    df = df[df["temp_c"].between(-60, 60)]
    df = df[df["humidity"].between(0, 100)]

    # ── 4. Add computed columns ────────────────────────────────
    df["feels_hot"]   = df["temp_c"] > 25
    df["wind_strong"] = df["wind_kmph"] > 40

    # ── 5. Save clean data ─────────────────────────────────────
    output_file = "data/clean_weather.csv"
    df.to_csv(output_file, index=False)

    log.info(f"Transform complete → {len(df)} clean rows saved to {output_file}")
    return df


if __name__ == "__main__":
    transform()
