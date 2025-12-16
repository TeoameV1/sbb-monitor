import requests
import sqlite3
import os

# paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'sbb.db')

STATION_NAME = 'Zürich HB'
API_URL = f"http://transport.opendata.ch/v1/stationboard?station={STATION_NAME}&limit=15"


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # stations table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                id TEXT PRIMARY KEY,
                name TEXT,
                x REAL,
                y REAL
            )
        """)

        # departures table (NO DUPLICATES)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS departures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT NOT NULL,
                departure_ts INTEGER NOT NULL,
                destination TEXT,
                category TEXT,
                train_nr TEXT,
                delay INTEGER,
                FOREIGN KEY (station_id) REFERENCES stations(id),
                UNIQUE (station_id, departure_ts)
            )
        """)

        # helpful index
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_departures_station_time
            ON departures (station_id, departure_ts)
        """)

        conn.commit()


def fetch_and_store():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # store station info
            s = data["station"]
            cursor.execute("""
                INSERT OR REPLACE INTO stations (id, name, x, y)
                VALUES (?, ?, ?, ?)
            """, (
                s["id"],
                s["name"],
                s["coordinate"]["x"],
                s["coordinate"]["y"]
            ))

            inserted = 0

            for train in data["stationboard"]:
                stop = train.get("stop", {})
                departure_ts = stop.get("departureTimestamp")

                # skip malformed entries
                if not departure_ts:
                    continue

                delay = stop.get("delay") or 0

                cursor.execute("""
                    INSERT OR IGNORE INTO departures
                    (station_id, departure_ts, destination, category, train_nr, delay)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    s["id"],
                    departure_ts,
                    train.get("to"),
                    train.get("category"),
                    train.get("number"),
                    delay
                ))

                if cursor.rowcount > 0:
                    inserted += 1

            conn.commit()
            print(f"Inserted {inserted} new departures.")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    init_db()
    print("fetch_and_store() called")
    fetch_and_store()
