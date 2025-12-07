from flask import Flask, render_template, jsonify
import sqlite3
import os

app = Flask(__name__)

# need to use absolute path or flask gets confused
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'sbb.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    conn = get_db()
    
    # figure out when we started collecting data
    start_row = conn.execute('SELECT MIN(timestamp) as start_time FROM departures').fetchone()
    start_date = start_row['start_time'] if start_row['start_time'] else "No data yet"

    # get the most popular destinations
    destinations = conn.execute('''
        SELECT destination, COUNT(*) as frequency 
        FROM departures 
        GROUP BY destination 
        ORDER BY frequency DESC 
        LIMIT 5
    ''').fetchall()

    # 3. Query Types (Distribution)
    types = conn.execute('''
        SELECT category, COUNT(*) as count 
        FROM departures 
        GROUP BY category
    ''').fetchall()
    
    conn.close()
    
    chart_dest = [{'label': r['destination'], 'val': r['frequency']} for r in destinations]
    chart_type = [{'label': r['category'], 'val': r['count']} for r in types]

    return render_template('index.html', d_data=chart_dest, t_data=chart_type, start_date=start_date)

@app.route('/reset', methods=['POST'])
def reset_data():
    try:
        conn = get_db()
        conn.execute('DELETE FROM departures') # wipe everything from the table
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)