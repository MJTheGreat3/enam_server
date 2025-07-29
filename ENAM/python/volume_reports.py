import os
import requests
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import logging

# === CONFIGURE LOGGING ===
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# === DB CONNECTION CONFIG ===
DB_CONFIG = {
    "dbname": "enam",
    "user": "postgres",
    "password": "mathew",
    "host": "localhost",
    "port": 5432
}

# === DB UTILS ===
def get_connection():
    logging.debug("Opening database connection...")
    return psycopg2.connect(**DB_CONFIG)

def execute_batch_insert(query, rows, commit=True):
    with get_connection() as conn:
        with conn.cursor() as cur:
            logging.debug(f"Executing batch insert of {len(rows)} rows")
            cur.executemany(query, rows)
        if commit:
            conn.commit()

def truncate_table(table_name):
    with get_connection() as conn:
        with conn.cursor() as cur:
            logging.debug(f"Truncating table: {table_name}")
            cur.execute(f"TRUNCATE {table_name}")
        conn.commit()

# === DOWNLOAD AND CLEAN ===
def get_last_11_weekdays():
    dates = []
    current = datetime.now() - timedelta(days=1)
    while len(dates) < 11:
        if current.weekday() < 5:
            dates.append(current.strftime("%d%m%Y"))
        current -= timedelta(days=1)
    logging.info(f"Last 11 weekdays: {dates}")
    return dates

def filter_eq_series_df(df):
    return df[df[" SERIES"] == " EQ"]

def download_and_prepare_files(dates, save_dir):
    base_url = "https://nsearchives.nseindia.com/products/content/"
    os.makedirs(save_dir, exist_ok=True)
    downloaded = []

    for date in dates:
        filename = f"sec_bhavdata_full_{date}.csv"
        url = base_url + filename
        local_path = os.path.join(save_dir, f"{date}.csv")

        if os.path.exists(local_path):
            logging.info(f"File already exists: {local_path}")
            downloaded.append(local_path)
            continue

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.nseindia.com/",
            "Accept-Language": "en-US,en;q=0.9",
        }

        logging.info(f"Downloading: {url}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(response.content)
            logging.info(f"Saved raw file: {local_path}")

            # Filter EQ series
            df = pd.read_csv(local_path)
            df = filter_eq_series_df(df)
            df.to_csv(local_path, index=False)
            logging.info(f"Filtered EQ series and saved: {local_path}")

            downloaded.append(local_path)
        else:
            logging.error(f"Failed to download {url}. Status: {response.status_code}")
            raise Exception(f"Failed to download {url}")
    return downloaded

# === MASTER CALCULATION AND UPLOAD ===
def update_master_table(csv_files):
    logging.info("Starting master table update process...")
    data = {}

    for file in csv_files:
        logging.debug(f"Processing: {file}")
        df = pd.read_csv(file, usecols=["SYMBOL", " TTL_TRD_QNTY", " DELIV_QTY"])

        df[" TTL_TRD_QNTY"] = pd.to_numeric(df[" TTL_TRD_QNTY"].astype(str).str.replace(",", ""), errors="coerce")
        df[" DELIV_QTY"] = pd.to_numeric(df[" DELIV_QTY"].astype(str).str.replace(",", ""), errors="coerce")
        df = df.dropna(subset=[" TTL_TRD_QNTY", " DELIV_QTY"])

        for _, row in df.iterrows():
            symbol = row["SYMBOL"]
            trd_qty = row[" TTL_TRD_QNTY"]
            deliv_qty = row[" DELIV_QTY"]

            if symbol not in data:
                data[symbol] = {"TTL_TRD_QNTY_SUM": 0, "DELIV_QTY_SUM": 0, "COUNT": 0}

            data[symbol]["TTL_TRD_QNTY_SUM"] += trd_qty
            data[symbol]["DELIV_QTY_SUM"] += deliv_qty
            data[symbol]["COUNT"] += 1

    avg_data = []
    for symbol, stats in data.items():
        avg_trd_qty = round(stats["TTL_TRD_QNTY_SUM"] / stats["COUNT"], 2)
        avg_deliv_qty = round(stats["DELIV_QTY_SUM"] / stats["COUNT"], 2)
        # Convert to TEXT for DB
        avg_data.append((symbol, str(avg_trd_qty), str(avg_deliv_qty)))

    logging.info(f"Inserting {len(avg_data)} rows into master table")
    truncate_table("master")
    insert_query = """
        INSERT INTO master (SYMBOL, AVG_TTL_TRD_QNTY, AVG_DELIV_QTY) VALUES (%s, %s, %s)
    """
    execute_batch_insert(insert_query, avg_data)
    logging.info("Master table updated successfully.")

# === DEVIATION CHECK ===
def compare_with_master_and_update(new_csv):
    logging.info("Starting deviation comparison...")

    new_df = pd.read_csv(new_csv, usecols=["SYMBOL", " TTL_TRD_QNTY", " DELIV_QTY"])
    new_df[" TTL_TRD_QNTY"] = pd.to_numeric(new_df[" TTL_TRD_QNTY"].astype(str).str.replace(",", ""), errors="coerce")
    new_df[" DELIV_QTY"] = pd.to_numeric(new_df[" DELIV_QTY"].astype(str).str.replace(",", ""), errors="coerce")
    new_df.dropna(subset=[" TTL_TRD_QNTY", " DELIV_QTY"], inplace=True)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT SYMBOL, AVG_TTL_TRD_QNTY, AVG_DELIV_QTY FROM master")
            master_data = {row[0]: (float(row[1]), float(row[2])) for row in cur.fetchall()}

    vol_devs = []
    deliv_devs = []

    for _, row in new_df.iterrows():
        symbol = row["SYMBOL"]
        new_trd = row[" TTL_TRD_QNTY"]
        new_deliv = row[" DELIV_QTY"]

        if symbol not in master_data:
            continue

        avg_trd, avg_deliv = master_data[symbol]

        if avg_trd and abs((new_trd - avg_trd) / avg_trd) >= 0.5:
            pct_dev = round(((new_trd - avg_trd) / avg_trd) * 100, 2)
            vol_devs.append((
                symbol,
                str(round(avg_trd, 2)),
                str(new_trd),
                pct_dev
            ))

        if avg_deliv and abs((new_deliv - avg_deliv) / avg_deliv) >= 0.5:
            pct_dev = round(((new_deliv - avg_deliv) / avg_deliv) * 100, 2)
            deliv_devs.append((
                symbol,
                str(round(avg_deliv, 2)),
                str(new_deliv),
                pct_dev
            ))

    logging.info(f"Found {len(vol_devs)} VOL deviations and {len(deliv_devs)} DELIV deviations")

    truncate_table("vol_deviation")
    truncate_table("deliv_deviation")

    insert_vol = """
        INSERT INTO vol_deviation (SYMBOL, AVG_TTL_TRD_QNTY, NEW_TTL_TRD_QNTY, PCT_DEVIATION)
        VALUES (%s, %s, %s, %s)
    """
    insert_deliv = """
        INSERT INTO deliv_deviation (SYMBOL, AVG_DELIV_QTY, NEW_DELIV_QTY, PCT_DEVIATION)
        VALUES (%s, %s, %s, %s)
    """

    if vol_devs:
        execute_batch_insert(insert_vol, vol_devs)
    if deliv_devs:
        execute_batch_insert(insert_deliv, deliv_devs)

    logging.info("Deviation tables updated successfully.")

# === MAIN ===
if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    VOLUME_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "volume_data_pg"))
    os.makedirs(VOLUME_DIR, exist_ok=True)

    last_11_dates = get_last_11_weekdays()

    try:
        downloaded_files = download_and_prepare_files(last_11_dates, VOLUME_DIR)
        logging.info(f"All files downloaded and filtered: {downloaded_files}")

        # Master average: use last 10 days
        update_master_table(downloaded_files[1:])

        # Deviation check: use most recent day
        compare_with_master_and_update(downloaded_files[0])

    except Exception as e:
        logging.error(f"Script failed: {e}")
