import psycopg2
from psycopg2.extras import DictCursor
from dateutil import parser
from datetime import datetime, timedelta

# === DATABASE CONFIG ===
DB_NAME = "enam"
DB_USER = "postgres"
DB_PASSWORD = "mathew"
DB_HOST = "localhost"
DB_PORT = 5432

# === Category Logic ===
ALLOWED_CATEGORIES_PRIORITY = [
    'Stock', 'IPOs', 'Companies', 'Markets', 'Economy',
    'Finance', 'Business', 'Industry', 'Technology',
    'Research', 'Other'
]

SPECIAL_WORD_MAPPING = {
    'money': 'Finance',
    'banking': 'Finance',
    'economic': 'Economy',
    'equity': 'Markets',
    'commodities': 'Industry',
    'commodity': 'Industry',
    'asset': 'Business',
    'earnings': 'Business'
}

def clean_time_string(time_str):
    if not time_str:
        return None
    time_str = time_str.strip()
    try:
        if "|" in time_str:
            time_part, date_part = [s.strip() for s in time_str.split("|", 1)]
            combined = f"{date_part} {time_part}"
            dt = parser.parse(combined)
        else:
            dt = parser.parse(time_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"[Warning] Could not parse time '{time_str}': {e}")
        return None

def normalize_category_word(word):
    word = word.lower().strip()
    if word.endswith('s') and not word.endswith('ss'):
        return word[:-1]
    return word

def map_single_category(raw_cat):
    raw_cat_clean = raw_cat.strip()
    if not raw_cat_clean:
        return "Other"

    cat_lower = raw_cat_clean.lower()

    for special_word, mapped in SPECIAL_WORD_MAPPING.items():
        if special_word in cat_lower:
            return mapped

    normalized_input = normalize_category_word(cat_lower)
    for allowed in ALLOWED_CATEGORIES_PRIORITY:
        if normalize_category_word(allowed.lower()) == normalized_input:
            return allowed

    for allowed in ALLOWED_CATEGORIES_PRIORITY:
        if allowed.lower() in cat_lower:
            return allowed

    return "Other"

def clean_category_string(category_str):
    if not category_str:
        return "Other"
    categories = [c.strip() for c in category_str.split(',') if c.strip()]
    cleaned = [map_single_category(cat) for cat in categories]

    seen = set()
    result = []
    for cat in cleaned:
        if cat not in seen:
            seen.add(cat)
            result.append(cat)
    return ', '.join(result)

def is_recent_enough(time_str, days=14):
    try:
        dt = parser.parse(time_str)
        cutoff = datetime.now() - timedelta(days=days)
        return dt >= cutoff
    except Exception as e:
        print(f"[Warning] Could not parse time for filtering '{time_str}': {e}")
        return False

def clean_news_table():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor(cursor_factory=DictCursor)

    cursor.execute("SELECT id, time, category FROM news")
    rows = cursor.fetchall()

    cleaned_count = 0
    deleted_count = 0

    for row in rows:
        row_id = row['id']
        raw_time = row['time']
        raw_cat = row['category']

        cleaned_time = clean_time_string(raw_time)
        if not cleaned_time or not is_recent_enough(cleaned_time):
            print(f"[INFO] Deleting old or invalid article: {raw_time}")
            cursor.execute("DELETE FROM news WHERE id = %s", (row_id,))
            deleted_count += 1
            continue

        cleaned_category = clean_category_string(raw_cat)

        if cleaned_time != raw_time or cleaned_category != raw_cat:
            cursor.execute(
                "UPDATE news SET time = %s, category = %s WHERE id = %s",
                (cleaned_time, cleaned_category, row_id)
            )
            cleaned_count += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[DONE] Cleaned: {cleaned_count} rows | Deleted: {deleted_count} rows")

if __name__ == "__main__":
    clean_news_table()
