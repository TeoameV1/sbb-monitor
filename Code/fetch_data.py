import requests
import sqlite3
import datetime
import os

# stuff we need for paths and config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'sbb.db')

STATION_NAME = 'Zürich HB' # TODO: maybe make this configurable later?
API_URL = f"http://transport.opendata.ch/v1/stationboard?station={STATION_NAME}&limit=15"

def init_db():
    # sets up the database if it doesn't exist yet
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        # first table is for station info
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stations (
                id TEXT PRIMARY KEY,
                name TEXT,
                x REAL,
                y REAL
            )
        ''')
        # second table stores all the departure times
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS departures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME,
                destination TEXT,
                category TEXT,
                train_nr TEXT,
                delay INTEGER,
                station_id TEXT,
                FOREIGN KEY(station_id) REFERENCES stations(id)
            )
        ''')
        conn.commit()

def fetch_and_store():
    try:
        # grab data from the SBB API
        response = requests.get(API_URL)
        data = response.json()

        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # save station info (replaces if already exists)
            s = data['station']
            cursor.execute("INSERT OR REPLACE INTO stations VALUES (?, ?, ?, ?)",
                           (s['id'], s['name'], s['coordinate']['x'], s['coordinate']['y']))

            # now loop through all trains and save them
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            count = 0
            for train in data['stationboard']:
                delay = train['stop']['delay']
                if delay is None: delay = 0  # some trains don't have delay info
                
                cursor.execute('''
                    INSERT INTO departures (timestamp, destination, category, train_nr, delay, station_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (now, train['to'], train['category'], train['number'], delay, s['id']))
                count += 1
            
            print(f"[{now}] Success: Stored {count} trains.")
            conn.commit()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    init_db()
    fetch_and_store()