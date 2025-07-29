import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import psycopg2

def get_connection():
    return psycopg2.connect(
        dbname="enam",
        user="postgres",
        password="mathew",
        host="localhost",
        port="5432"
    )


# ----------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------
BASE_URL = "https://www.financialexpress.com/latest-news/"
SOURCE = "Financial Express"
TIME_LIMIT = datetime.now(timezone.utc) - timedelta(hours=24)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

VALID_CATEGORIES = {
    "banking-finance",
    "business",
    "economy",
    "industry",
    "ipo-news",
    "market"
}

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def filter_categories(category_string):
    cats = category_string.split(",")
    valid = [c for c in cats if c in VALID_CATEGORIES]
    return ",".join(valid)

def parse_categories(class_list):
    return ",".join(
        [cls.split("category-")[1] for cls in class_list if cls.startswith("category-")]
    )

def parse_article_div1(article):
    raw_categories = parse_categories(article.get("class", []))
    filtered_categories = filter_categories(raw_categories)
    if not filtered_categories:
        return None

    headline_tag = article.find("div", class_="entry-title")
    headline = headline_tag.get_text(strip=True) if headline_tag else ""
    link = headline_tag.find("a")["href"] if headline_tag and headline_tag.find("a") else ""
    return {
        "Source": SOURCE,
        "Headline": headline,
        "Link": link,
        "Category": filtered_categories,
        "Time": ""
    }

def parse_article_div2(article):
    raw_categories = parse_categories(article.get("class", []))
    filtered_categories = filter_categories(raw_categories)
    if not filtered_categories:
        return None, None

    title_tag = article.find("div", class_="entry-title")
    headline = title_tag.get_text(strip=True) if title_tag else ""
    link = title_tag.find("a")["href"] if title_tag and title_tag.find("a") else ""
    time_tag = article.find("time")
    timestr = ""
    art_datetime = None
    if time_tag and time_tag.has_attr("datetime"):
        timestr = time_tag["datetime"]
        try:
            art_datetime = datetime.fromisoformat(timestr).astimezone(timezone.utc)
        except ValueError:
            art_datetime = None
    return {
        "Source": SOURCE,
        "Headline": headline,
        "Link": link,
        "Category": filtered_categories,
        "Time": timestr
    }, art_datetime

def get_soup(url):
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return BeautifulSoup(resp.content, "html.parser")

# ----------------------------------------------------------------------------
# Database Helpers
# ----------------------------------------------------------------------------
def get_existing_links(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT link FROM news WHERE source = %s", (SOURCE,))
        return {row[0] for row in cur.fetchall()}

def insert_articles(conn, records):
    inserted = 0
    with conn.cursor() as cur:
        for record in records:
            cur.execute("""
                INSERT INTO news (source, headline, link, category, time)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING
                RETURNING link
            """, (
                record["Source"],
                record["Headline"],
                record["Link"],
                record["Category"],
                record["Time"]
            ))
            if cur.fetchone():
                inserted += 1
    conn.commit()
    return inserted

# ----------------------------------------------------------------------------
# Main Scrape
# ----------------------------------------------------------------------------
def scrape():
    page = 1
    stop_scraping = False
    all_new_records = []

    conn = get_connection()
    existing_links = get_existing_links(conn)

    while not stop_scraping:
        url = BASE_URL if page == 1 else f"{BASE_URL}page/{page}/"
        try:
            soup = get_soup(url)
        except Exception:
            break

        story_divs = soup.find_all("div", class_="wp-block-newspack-blocks-ie-stories")
        if len(story_divs) < 2:
            break

        # div 1
        first_div = story_divs[0]
        first_article = first_div.find("article")
        if first_article:
            record = parse_article_div1(first_article)
            if record and record["Link"] not in existing_links:
                all_new_records.append(record)
            else:
                stop_scraping = True
                break

        # div 2
        second_div = story_divs[1]
        articles = second_div.find_all("article")
        for art in articles:
            record, art_time = parse_article_div2(art)
            if not record:
                continue
            if record["Link"] in existing_links:
                stop_scraping = True
                break
            if art_time and art_time < TIME_LIMIT:
                stop_scraping = True
                break

            all_new_records.append(record)

        if stop_scraping:
            break

        page += 1
        time.sleep(1)

    if all_new_records:
        inserted_count = insert_articles(conn, all_new_records)
        print(f"... {inserted_count} articles added from Financial Express ...")
    else:
        print("... No new Financial Express articles found ...")

    conn.close()

# ----------------------------------------------------------------------------
if __name__ == "__main__":
    scrape()
