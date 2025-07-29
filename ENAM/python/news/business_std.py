import time
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import psycopg2

# ----------------------------------------------------------------------------
# Constants
SOURCE = "Business Standard"
BASE_URL = "https://www.business-standard.com/latest-news"
ALLOWED_CATEGORIES = {"companies", "economy", "markets", "industry", "finance"}
MAX_AGE_HOURS = 24

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "enam"
DB_USER = "postgres"
DB_PASSWORD = "mathew"

NOW_UTC = datetime.now(timezone.utc)
TIME_CUTOFF = NOW_UTC - timedelta(hours=MAX_AGE_HOURS)

# ----------------------------------------------------------------------------
def safe_print(*args, **kwargs):
    text = " ".join(str(arg) for arg in args)
    try:
        print(text, **kwargs)
    except UnicodeEncodeError:
        print(text.encode('ascii', errors='replace').decode('ascii'), **kwargs)

# ----------------------------------------------------------------------------
def parse_bs_timestamp(time_text):
    try:
        time_text = time_text.replace("Updated On :", "").strip().lower().replace('ist', '').strip()
        is_premium = 'premium' in time_text
        time_text = time_text.replace('premium', '').strip()

        parts = time_text.split('|')
        if len(parts) != 2:
            return None, False

        date_part = parts[0].strip().title()
        time_part = parts[1].strip().upper()
        clean_time_str = f"{date_part} | {time_part}"

        dt = datetime.strptime(clean_time_str, "%d %b %Y | %I:%M %p")
        return dt.replace(tzinfo=timezone.utc), is_premium
    except Exception:
        return None, False

# ----------------------------------------------------------------------------
def get_existing_links_from_db(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT link FROM news WHERE source = %s", (SOURCE,))
        return set(row[0] for row in cur.fetchall())

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
def extract_articles_from_soup(soup, existing_links, cutoff_datetime):
    container = soup.find('div', class_='article-listing')
    if not container:
        return [], True

    articles = container.find_all('div', class_='listingstyle_cardlistlist__dfq57 cardlist')
    new_entries = []
    stop = False

    for article in articles:
        headline_tag = article.find('a', class_='smallcard-title')
        if not headline_tag:
            continue

        link = headline_tag['href'].strip()
        headline = headline_tag.text.strip()

        if link in existing_links:
            safe_print("[SCRAPER][STOP] Duplicate article:", link)
            stop = True
            break

        time_div = article.find('div', class_='listingstyle_timestmp__VSJNW')
        raw_time_text = time_div.get_text(strip=True) if time_div else ""
        parsed_dt, is_premium = parse_bs_timestamp(raw_time_text)

        if not parsed_dt:
            safe_print("[SCRAPER][WARN] Could not parse date:", raw_time_text)
            continue

        if parsed_dt < cutoff_datetime:
            safe_print("[SCRAPER][STOP] Older than 24h:", raw_time_text)
            stop = True
            break

        if is_premium:
            headline = f"[Premium] {headline}"

        try:
            category = link.split("/")[3].lower() if len(link.split("/")) > 3 else ""
        except IndexError:
            category = ""

        if category not in ALLOWED_CATEGORIES:
            safe_print("[SCRAPER][SKIP] Category not allowed:", category, "|", headline)
            continue

        safe_print("[SCRAPER][NEW]", category, "|", headline)

        new_entries.append({
            "Source": SOURCE,
            "Headline": headline,
            "Link": link,
            "Category": category,
            "Time": parsed_dt.strftime("%Y-%m-%d %H:%M:%S")
        })

    return new_entries, stop

# ----------------------------------------------------------------------------
def main():
    safe_print("[SCRAPER] Starting Business Standard Scraper")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except Exception as e:
        safe_print("[ERROR] DB connection failed:", e)
        return

    existing_links = get_existing_links_from_db(conn)
    safe_print(f"[SCRAPER] Loaded {len(existing_links)} existing links for {SOURCE}")

    chrome_options = Options()
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(options=chrome_options)

    all_new_entries = []
    page_number = 1
    stop_scraping = False

    while not stop_scraping:
        page_url = BASE_URL if page_number == 1 else f"{BASE_URL}/page-{page_number}"
        driver.get(page_url)
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        new_entries, stop_scraping = extract_articles_from_soup(soup, existing_links, TIME_CUTOFF)

        all_new_entries.extend(new_entries)
        page_number += 1

    driver.quit()

    if not all_new_entries:
        safe_print("[SCRAPER] No new articles found.")
        conn.close()
        return

    insert_articles_to_db(conn, all_new_entries)
    safe_print(f"[SCRAPER][SAVE] {len(all_new_entries)} articles added.")

    conn.close()
    safe_print("[SCRAPER] Done.")

# ----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
