import datetime
import psycopg2

DB_CONFIG = {
    "dbname": "enam",
    "user": "postgres",
    "password": "mathew",
    "host": "localhost",
    "port": "5432"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def set_last_updated(key):
    conn = get_db_connection()
    with conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO last_updated (key, timestamp)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET timestamp = EXCLUDED.timestamp;
        """, (key, datetime.datetime.now()))
    conn.close()

def get_last_updated(key):
    conn = get_db_connection()
    with conn, conn.cursor() as cur:
        cur.execute("SELECT timestamp FROM last_updated WHERE key = %s;", (key,))
        result = cur.fetchone()
    conn.close()
    return result[0].strftime("%Y-%m-%d %H:%M:%S") if result else None
