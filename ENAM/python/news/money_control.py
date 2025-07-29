import requests
from bs4 import BeautifulSoup, Comment
import threading
import time
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

# === CONFIG ===
SOURCE = "Money Control"
DATE_CUTOFF = datetime.now() - timedelta(hours=24)

CATEGORIES = {
    "economy": "https://www.moneycontrol.com/news/business/economy",
    "companies": "https://www.moneycontrol.com/news/business/companies",
    "ipo": "https://www.moneycontrol.com/news/business/ipo",
    "stocks": "https://www.moneycontrol.com/news/business/stocks",
    "commodities": "https://www.moneycontrol.com/news/business/commodities",
    "real-estate": "https://www.moneycontrol.com/news/business/real-estate",
}

csv_lock = threading.Lock()

# === DB CONNECTION ===
def get_connection():
    return psycopg2.connect(
        dbname="enam",
        user="postgres",
        password="mathew",
        host="localhost",
        port="5432"
    )

def get_existing_links():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT link FROM news WHERE source = %s", (SOURCE,))
            return set(row[0] for row in cur.fetchall())

def insert_new_articles(records):
    with get_connection() as conn:
        with conn.cursor() as cur:
            values = [(SOURCE, r["Headline"], r["Link"], r["Category"], r["Time"]) for r in records]
            query = """
                INSERT INTO news (source, headline, link, category, time)
                VALUES %s
                ON CONFLICT (link) DO NOTHING;
            """
            execute_values(cur, query, values)
        conn.commit()

# === TIME PARSER ===
def parse_time(text):
    text = text.replace(' IST','').strip()
    try:
        return datetime.strptime(text, "%B %d, %Y %I:%M %p")
    except Exception:
        return None

# === SCRAPE ONE PAGE ===
def scrape_page(url, category, existing_links):
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if resp.status_code != 200:
            return [], False
    except Exception:
        return [], False

    soup = BeautifulSoup(resp.text, 'html.parser')
    articles = soup.find_all('li', class_='clearfix')

    rows = []
    stop_signal = False

    for art in articles:
        try:
            a_tag = art.find('a', href=True, title=True)
            if not a_tag:
                continue

            link = a_tag['href']
            headline = a_tag['title'].strip()

            # === TIME EXTRACTION ===
            time_text = ""
            time_span = art.find('span')
            if time_span:
                time_text = time_span.get_text(strip=True)

            if not time_text:
                comments = art.find_all(string=lambda text: isinstance(text, Comment))
                for comment in comments:
                    if '<span>' in comment:
                        soup2 = BeautifulSoup(comment, 'html.parser')
                        span = soup2.find('span')
                        if span:
                            time_text = span.get_text(strip=True)
                            break

            if not time_text:
                continue

            dt = parse_time(time_text)
            if not dt:
                continue

            if dt < DATE_CUTOFF:
                stop_signal = True
                break

            with csv_lock:
                if link in existing_links:
                    stop_signal = True
                    break

            rows.append({
                "Headline": headline,
                "Link": link,
                "Category": category,
                "Time": dt.isoformat()
            })

        except Exception:
            continue

    return rows, stop_signal

# === CATEGORY SCRAPER ===
def scrape_category(category, base_url, existing_links, results_lock, all_new_records):
    page_num = 1

    while True:
        page_url = base_url if page_num == 1 else f"{base_url}/page-{page_num}"
        rows, stop = scrape_page(page_url, category, existing_links)

        if rows:
            with results_lock:
                all_new_records.extend(rows)
                existing_links.update([r["Link"] for r in rows])

        if stop or not rows:
            break

        page_num += 1
        time.sleep(1)

# === MAIN ===
def main():
    existing_links = get_existing_links()
    results_lock = threading.Lock()
    all_new_records = []

    threads = []
    for cat, url in CATEGORIES.items():
        t = threading.Thread(target=scrape_category, args=(cat, url, existing_links, results_lock, all_new_records))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if all_new_records:
        insert_new_articles(all_new_records)
        print(f"[{SOURCE}] Inserted {len(all_new_records)} new articles.")
    else:
        print(f"[{SOURCE}] No new articles found.")

if __name__ == "__main__":
    main()
