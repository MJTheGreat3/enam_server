import requests
import pandas as pd
import os
import psycopg2
import logging
from datetime import datetime, timedelta

# === LOGGING CONFIGURATION ===
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# === DB CONFIG ===
DB_CONFIG = {
    "dbname": "enam",
    "user": "postgres",
    "password": "mathew",
    "host": "localhost",
    "port": 5432
}

# === DB UTILS ===
def get_connection():
    logging.debug("Connecting to database...")
    return psycopg2.connect(**DB_CONFIG)

def truncate_table(table):
    with get_connection() as conn:
        with conn.cursor() as cur:
            logging.debug(f"Truncating table: {table}")
            cur.execute(f"TRUNCATE {table}")
        conn.commit()

def batch_insert_corp_actions(rows):
    logging.debug(f"Inserting {len(rows)} rows into corp_actions...")
    query = """
        INSERT INTO corp_actions (
            Security_Code, Security_Name, Company_Name, Ex_Date, Purpose,
            Record_Date, BC_Start_Date, BC_End_Date, ND_Start_Date, ND_End_Date, Actual_Payment_Date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, rows)
        conn.commit()
    logging.info("Data inserted into corp_actions successfully.")

# === DOWNLOAD CSV ===
def download_bse_csv(temp_file):
    url = "https://api.bseindia.com/BseIndiaAPI/api/CorpactCSVDownload/w"
    params = {
        "scripcode": "",
        "Fdate": "",
        "TDate": "",
        "Purposecode": "",
        "strSearch": "S",
        "ddlindustrys": "",
        "ddlcategorys": "E",
        "segment": "0"
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/138.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.bseindia.com/"
    }

    logging.info("Requesting CSV from BSE...")
    response = requests.get(url, headers=headers, params=params)
    if response.ok:
        with open(temp_file, "wb") as f:
            f.write(response.content)
        logging.info(f"Downloaded BSE CSV to {temp_file}")
    else:
        logging.error(f"Failed to download. Status: {response.status_code}")
        raise Exception(f"Failed to download BSE CSV")

# === DATE PARSING ===
def parse_date_safe(date_str):
    """
    Parse BSE date format like '15 Jul 2025'.
    Return None if missing or invalid.
    """
    if not isinstance(date_str, str):
        return None
    date_str = date_str.strip()
    if not date_str:
        return None
    try:
        parsed = datetime.strptime(date_str, "%d %b %Y").date()
        return parsed
    except ValueError:
        return None

# === FILTER BY DATE WINDOW ===
def filter_by_date_window(df):
    logging.debug("Filtering by 2-month window on Record Date / Ex Date...")
    today = datetime.today().date()
    two_months = today + timedelta(days=61)

    record_dates = df['Record Date'].apply(parse_date_safe)
    ex_dates = df['Ex Date'].apply(parse_date_safe)

    def is_in_range(date_val):
        if date_val is None:
            return False
        return today <= date_val <= two_months

    mask = record_dates.apply(is_in_range) | ex_dates.apply(is_in_range)
    filtered = df[mask]
    logging.debug(f"Rows after filtering: {len(filtered)}")
    return filtered

# === MAIN ===
def main():
    logging.info("Starting Corporate Actions pipeline...")

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    TEMP_CSV = os.path.join(SCRIPT_DIR, "downloaded_bse.csv")

    # 1. Download
    download_bse_csv(TEMP_CSV)

    # 2. Load
    logging.debug("Loading downloaded CSV...")
    df = pd.read_csv(TEMP_CSV)
    logging.debug(f"Loaded {len(df)} rows from downloaded CSV.")

    # 3. CLEAN HEADERS
    df.columns = df.columns.str.strip()
    logging.debug(f"Cleaned columns: {list(df.columns)}")

    if df.empty:
        logging.warning("Downloaded CSV is empty. Exiting.")
        os.remove(TEMP_CSV)
        return

    # 4. Drop duplicates early
    df = df.drop_duplicates()
    logging.debug(f"Rows after initial deduplication: {len(df)}")

    # 5. Filter for date window
    df = filter_by_date_window(df)

    if df.empty:
        logging.warning("No rows within 2-month window. Exiting.")
        os.remove(TEMP_CSV)
        return

    # 6. Keep only desired columns
    desired_cols = [
        "Security Code","Security Name","Company Name","Ex Date","Purpose",
        "Record Date","BC Start Date","BC End Date","ND Start Date","ND End Date","Actual Payment Date"
    ]
    df = df[desired_cols]

    # 7. Convert date columns to DATE
    date_cols = [
        "Ex Date", "Record Date", "BC Start Date", "BC End Date",
        "ND Start Date", "ND End Date", "Actual Payment Date"
    ]
    for col in date_cols:
        df[col] = df[col].apply(parse_date_safe)

    # 8. Final deduplication (after date parsing)
    df = df.drop_duplicates()
    logging.debug(f"Rows after final deduplication: {len(df)}")

    # 9. Prepare rows for DB
    rows = [
        (
            row["Security Code"], row["Security Name"], row["Company Name"],
            row["Ex Date"], row["Purpose"], row["Record Date"],
            row["BC Start Date"], row["BC End Date"],
            row["ND Start Date"], row["ND End Date"],
            row["Actual Payment Date"]
        )
        for _, row in df.iterrows()
    ]

    if not rows:
        logging.warning("No valid rows to insert. Exiting.")
        os.remove(TEMP_CSV)
        return

    # 10. Upload to DB
    truncate_table("corp_actions")
    batch_insert_corp_actions(rows)

    # 11. Cleanup
    os.remove(TEMP_CSV)
    logging.info(f"Deleted temp file {TEMP_CSV}")

    logging.info("Corporate Actions pipeline completed successfully.")

# === ENTRY POINT ===
if __name__ == "__main__":
    main()
