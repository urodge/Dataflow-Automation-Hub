"""
extract.py — Step 1 of the Weather ETL Pipeline
Fetches live weather data from the wttr.in public API and saves to CSV.
"""

import requests
import csv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def extract_weather(city: str = "London") -> str:
    """
    Fetches hourly weather data for a given city.
    Returns path to the saved CSV file.
    """
    url = f"https://wttr.in/{city}?format=j1"
    log.info(f"Fetching weather data for city: {city}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raises error for 4xx / 5xx responses
    except requests.exceptions.RequestException as e:
        log.error(f"API request failed: {e}")
        raise

    data = response.json()
    output_file = "data/raw_weather.csv"

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["city", "temp_c", "humidity", "wind_kmph", "description"])

        for hourly in data["weather"][0]["hourly"]:
            writer.writerow([
                city,
                hourly.get("tempC", ""),
                hourly.get("humidity", ""),
                hourly.get("windspeedKmph", ""),
                hourly["weatherDesc"][0].get("value", "") if hourly.get("weatherDesc") else ""
            ])

    log.info(f"Extract complete → saved to {output_file}")
    return output_file


if __name__ == "__main__":
    extract_weather("London")
