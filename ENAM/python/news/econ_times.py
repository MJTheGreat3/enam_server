import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import psycopg2

def get_connection():
    return psycopg2.connect(
        dbname="enam",
        user="postgres",
        password="mathew",
        host="localhost",
        port="5432"
    )

# === Settings ===
SOURCE = "Economic Times"
TIME_LIMIT = datetime.now(timezone.utc) - timedelta(hours=24)
START_URL = "https://economictimes.indiatimes.com/news/latest-news"
ALLOWED_CATEGORIES = {"markets", "stocks", "ipos", "economy", "finance"}

# === Category Parsing ===
def parse_category_from_link(link):
    try:
        path = urlparse(link).path
        parts = path.strip("/").split("/")
        if parts and "articleshow" in parts[-1]:
            parts = parts[:-1]
        if not parts:
            return ""
        parts = [p.lower() for p in parts]
        if "economy" in parts:
            return "economy"
        if "finance" in parts:
            return "finance"
        if "ipos" in parts:
            return "ipos"
        if len(parts) >= 2 and parts[0] == "markets" and parts[1] == "stocks":
            return "stocks"
        if "markets" in parts:
            return "markets"
        return parts[0]
    except Exception:
        return ""

def is_allowed_category(category):
    return category in ALLOWED_CATEGORIES

# === HTML Parsing ===
def extract_articles_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    ul_data = soup.find("ul", class_="data")
    if not ul_data:
        return []
    return ul_data.find_all("li", recursive=False)

def parse_article_li(li_tag):
    try:
        a_tag = li_tag.find("a", href=True)
        if not a_tag:
            return None, None

        headline = a_tag.get_text(strip=True)
        link = a_tag["href"]
        if not link.startswith("http"):
            link = "https://economictimes.indiatimes.com" + link

        category = parse_category_from_link(link)

        time_tag = li_tag.find("span", class_="timestamp")
        timestr = time_tag["data-time"] if time_tag and time_tag.has_attr("data-time") else ""

        art_datetime = None
        if timestr:
            try:
                art_datetime = datetime.fromisoformat(timestr.replace("Z", "+00:00")).astimezone(timezone.utc)
            except ValueError:
                art_datetime = None

        return {
            "Source": SOURCE,
            "Headline": headline,
            "Link": link,
            "Category": category,
            "Time": art_datetime.strftime("%Y-%m-%d %H:%M:%S") if art_datetime else None
        }, art_datetime
    except Exception:
        return None, None

# === Existing Links from DB ===
def fetch_existing_links():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT link FROM news WHERE source = %s", (SOURCE,))
                return set(row[0] for row in cur.fetchall())
    except Exception as e:
        print(f"[ERROR][DB] Failed to fetch existing links: {e}")
        return set()

def insert_articles_to_db(articles):
    if not articles:
        return
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for article in articles:
                    cur.execute(
                        """
                        INSERT INTO news (source, headline, link, category, time)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (link) DO NOTHING
                        """,
                        (
                            article["Source"],
                            article["Headline"],
                            article["Link"],
                            article["Category"],
                            article["Time"]
                        )
                    )
            conn.commit()
        print(f"[SCRAPER][SAVE] {len(articles)} articles added from Economic Times.")
    except Exception as e:
        print(f"[ERROR][DB] Failed to insert articles: {e}")

# === Main Scraping ===
def main():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(START_URL)
    time.sleep(3)

    existing_links = fetch_existing_links()
    all_new_records = []
    seen_links = set()
    stop_scraping = False

    SCROLL_AMOUNT = 300
    SCROLL_WAIT = 1.0
    MAX_SCROLLS = 100
    scroll_count = 0
    last_height = driver.execute_script("return document.body.scrollHeight")

    while not stop_scraping and scroll_count < MAX_SCROLLS:
        driver.execute_script(f"window.scrollBy(0, {SCROLL_AMOUNT});")
        time.sleep(SCROLL_WAIT)

        page_source = driver.page_source
        articles = extract_articles_from_html(page_source)

        for li in articles:
            record, art_time = parse_article_li(li)
            if not record:
                continue

            if record["Link"] in existing_links or record["Link"] in seen_links:
                stop_scraping = True
                break

            if art_time and art_time < TIME_LIMIT:
                stop_scraping = True
                break

            if not is_allowed_category(record["Category"]):
                continue

            all_new_records.append(record)
            seen_links.add(record["Link"])

        if stop_scraping:
            break

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break

        last_height = new_height
        scroll_count += 1

    driver.quit()

    if all_new_records:
        insert_articles_to_db(all_new_records)
    else:
        print("[SCRAPER][SAVE] 0 articles added from Economic Times.")

if __name__ == "__main__":
    main()
