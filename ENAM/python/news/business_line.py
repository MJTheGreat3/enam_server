import time
from datetime import datetime, timedelta, timezone
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ----------------------------------------------------------------------------
# Constants
SOURCE = "Hindu Business Line"
ALLOWED_CATEGORIES = {
    "Commodities",
    "Companies",
    "Economy",
    "Markets",
    "Money & Banking",
    "Stocks"
}

URL = "https://www.thehindubusinessline.com/latest-news/"
NOW_UTC = datetime.now(timezone.utc)
TIME_CUTOFF = NOW_UTC - timedelta(hours=24)

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "enam"
DB_USER = "postgres"
DB_PASSWORD = "mathew"

# ----------------------------------------------------------------------------
def safe_print(*args, **kwargs):
    text = " ".join(str(arg) for arg in args)
    try:
        print(text, **kwargs)
    except UnicodeEncodeError:
        print(text.encode('ascii', errors='replace').decode('ascii'), **kwargs)

# ----------------------------------------------------------------------------
def parse_article_time(time_str):
    for fmt in ["%H:%M | %b %d, %Y", "%H:%M | %B %d, %Y"]:
        try:
            dt = datetime.strptime(time_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

# ----------------------------------------------------------------------------
def get_existing_links_from_db(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT link FROM news WHERE source = %s",
            (SOURCE,)
        )
        rows = cur.fetchall()
        return set(row[0] for row in rows)

# ----------------------------------------------------------------------------
def insert_articles_to_db(conn, articles):
    with conn.cursor() as cur:
        for article in articles:
            cur.execute(
                """
                INSERT INTO news (source, headline, link, category, time)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING
                """,
                (article['Source'], article['Headline'], article['Link'], article['Category'], article['Time'])
            )
    conn.commit()

# ----------------------------------------------------------------------------
def main():
    safe_print("[SCRAPER] Starting HBL Scraper")

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except Exception as e:
        safe_print("[ERROR] Failed to connect to DB:", e)
        return

    existing_links = get_existing_links_from_db(conn)
    safe_print(f"[SCRAPER] Loaded {len(existing_links)} existing links for source {SOURCE}")

    # Setup headless Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(URL)
    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    news_div = soup.find('div', class_='fgdf')
    if not news_div:
        safe_print("[SCRAPER][ERROR] No news container found.")
        conn.close()
        return

    news_items = news_div.find_all('li')
    new_entries = []

    for item in news_items:
        a_tag = item.find('a', href=True)
        if not a_tag:
            continue

        relative_link = a_tag['href'].strip()
        link = urljoin(URL, relative_link)

        label = a_tag.find('div', class_='label').text.strip() if a_tag.find('div', class_='label') else ""
        time_str = a_tag.find('div', class_='time').text.strip() if a_tag.find('div', class_='time') else ""
        title = a_tag.find('h3', class_='title').text.strip() if a_tag.find('h3', class_='title') else ""

        article_dt = parse_article_time(time_str)
        if not article_dt:
            safe_print("[SCRAPER][WARN] Unable to parse time:", time_str)
            continue

        if article_dt < TIME_CUTOFF:
            safe_print("[SCRAPER][STOP] Older than 24h:", time_str)
            break

        if link in existing_links:
            safe_print("[SCRAPER][STOP] Duplicate article:", link)
            break

        if label not in ALLOWED_CATEGORIES:
            safe_print("[SCRAPER][SKIP] Category not allowed:", label, "|", title)
            continue

        safe_print("[SCRAPER][NEW]", label, "|", title)

        new_entries.append({
            "Source": SOURCE,
            "Headline": title,
            "Link": link,
            "Category": label,
            "Time": time_str
        })

    if not new_entries:
        safe_print("[SCRAPER] No new articles found.")
        conn.close()
        return

    insert_articles_to_db(conn, new_entries)
    safe_print(f"[SCRAPER][SAVE] {len(new_entries)} new articles added.")

    conn.close()
    safe_print("[SCRAPER] Done.")

# ----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
