import csv
import psycopg2
import re

CSV_FILE = "stock.csv"

DB_CONFIG = {
    "dbname": "enam",
    "user": "postgres",
    "password": "mathew",
    "host": "localhost",
    "port": 5432
}

def get_existing_isins_and_tag_ids(cursor):
    cursor.execute("SELECT ISIN, Tag_ID FROM symbols;")
    rows = cursor.fetchall()
    isins = {row[0] for row in rows}
    tag_ids = {row[1] for row in rows}
    return isins, tag_ids

def get_next_tag_id(existing_tag_ids):
    used_nums = sorted(
        int(re.match(r"T(\d+)", tid).group(1))
        for tid in existing_tag_ids if re.match(r"T\d+", tid)
    )
    next_num = 1
    for num in used_nums:
        if num == next_num:
            next_num += 1
        else:
            break
    return f"T{next_num:03d}"

def insert_new_symbol(cursor, tag_id, isin, symbol, name, alias):
    cursor.execute("""
        INSERT INTO symbols (Tag_ID, ISIN, Symbol, Name, Alias, Status, last_scraped)
        VALUES (%s, %s, %s, %s, %s, FALSE, NOW());
    """, (tag_id, isin, symbol, name, alias))

def main():
    with open(CSV_FILE, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

    conn = psycopg2.connect(**DB_CONFIG)
    with conn:
        with conn.cursor() as cur:
            existing_isins, existing_tag_ids = get_existing_isins_and_tag_ids(cur)

            new_entries = 0
            for row in rows:
                isin = row["ISIN"].strip()
                symbol = row["Symbol"].strip().upper()
                name = row["Name"].strip()
                alias = row["Alias"].strip()

                if isin in existing_isins:
                    continue  # Skip if ISIN already exists

                tag_id = get_next_tag_id(existing_tag_ids)
                existing_tag_ids.add(tag_id)

                insert_new_symbol(cur, tag_id, isin, symbol, name, alias)
                new_entries += 1

            print(f"Inserted {new_entries} new symbols.")

    conn.close()

if __name__ == "__main__":
    main()
