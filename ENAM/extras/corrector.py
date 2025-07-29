import psycopg2
import re

DB_CONFIG = {
    "dbname": "enam",
    "user": "postgres",
    "password": "mathew",
    "host": "localhost",
    "port": 5432
}

def convert_tag_id(old_tag_id):
    match = re.match(r"^T(\d{3})$", old_tag_id)
    if match:
        number = int(match.group(1))
        return f"T{number:04d}"
    return None

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT Tag_ID FROM symbols;")
            rows = cur.fetchall()

            updates = 0
            for (old_tag_id,) in rows:
                new_tag_id = convert_tag_id(old_tag_id)
                if new_tag_id:
                    # Check for conflicts
                    cur.execute("SELECT 1 FROM symbols WHERE Tag_ID = %s;", (new_tag_id,))
                    if cur.fetchone():
                        print(f"Skipping {old_tag_id} → {new_tag_id}: already exists")
                        continue

                    cur.execute("""
                        UPDATE symbols SET Tag_ID = %s WHERE Tag_ID = %s;
                    """, (new_tag_id, old_tag_id))
                    updates += 1
                    print(f"Updated {old_tag_id} → {new_tag_id}")

            print(f"Completed. {updates} Tag_IDs updated.")

    conn.close()

if __name__ == "__main__":
    main()
