from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import psycopg2
from psycopg2.extras import execute_values
import os

# ------------------ CONFIG ------------------
BASE_URL = "https://www.ft.com/news-feed"
SOURCE_NAME = "Financial Times"

ALLOWED_CATEGORIES = [
    "Markets", "Banking", "Asset", "Business", "Stock",
    "Companies", "Investment", "Private equity", "Economy", "Oil & Gas"
]

CUTOFF_TIME = datetime.now() - timedelta(hours=24)

# ------------------ DB CONNECTION ------------------
def get_connection():
    return psycopg2.connect(
        dbname="enam",
        user="postgres",
        password="mathew",
        host="localhost",
        port="5432"
    )

# ------------------ SETUP CHROME ------------------
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
driver = webdriver.Chrome(options=options)

# ------------------ HELPERS ------------------
def is_relevant_category(category_text):
    return any(key.lower() in category_text.lower() for key in ALLOWED_CATEGORIES)

def parse_article_date(date_text):
    try:
        return datetime.strptime(date_text, "%B %d %Y")
    except Exception:
        return None

def get_existing_links_and_headlines():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT link, headline FROM news WHERE source = %s", (SOURCE_NAME,))
            rows = cur.fetchall()
            existing_urls = {r[0] for r in rows}
            existing_titles = {r[1] for r in rows}
    return existing_urls, existing_titles

def insert_articles(articles):
    with get_connection() as conn:
        with conn.cursor() as cur:
            values = [(SOURCE_NAME, a["Headline"], a["Link"], a["Category"], a["Time"]) for a in articles]
            query = """
                INSERT INTO news (source, headline, link, category, time)
                VALUES %s
                ON CONFLICT (link) DO NOTHING;
            """
            execute_values(cur, query, values)
        conn.commit()

def parse_page_articles(page_source, existing_urls, existing_titles):
    soup = BeautifulSoup(page_source, 'html.parser')
    items = soup.find_all("li", class_="o-teaser-collection__item")
    articles = []
    stop_scraping = False

    for item in items:
        # --- Date parsing ---
        time_tag = item.find("time")
        if not time_tag:
            continue

        date_text = time_tag.get_text(strip=True)
        article_date = parse_article_date(date_text)
        if not article_date:
            continue

        if article_date < CUTOFF_TIME:
            stop_scraping = True
            break

        # --- Category parsing ---
        category_tag = item.find("a", class_="o-teaser__tag")
        category = category_tag.get_text(strip=True) if category_tag else "Uncategorized"
        if not is_relevant_category(category):
            continue

        # --- Headline and Link ---
        headline_tag = item.find("a", class_="js-teaser-heading-link")
        if not headline_tag or not headline_tag.has_attr("href"):
            continue

        url = "https://www.ft.com" + headline_tag['href']
        headline = headline_tag.get_text(strip=True)

        # --- Duplicate check ---
        if url in existing_urls or headline in existing_titles:
            stop_scraping = True
            break

        # --- Add article ---
        articles.append({
            "Headline": headline,
            "Link": url,
            "Category": category,
            "Time": article_date.strftime("%Y-%m-%d %H:%M:%S")
        })

    return articles, stop_scraping

# ------------------ MAIN SCRAPER ------------------
existing_urls, existing_titles = get_existing_links_and_headlines()

all_articles = []
page = 1
while True:
    print(f"Scraping FT page {page}")
    url = f"{BASE_URL}?page={page}"
    driver.get(url)
    time.sleep(5)

    new_articles, stop = parse_page_articles(driver.page_source, existing_urls, existing_titles)
    print(f"Found {len(new_articles)} new articles on page {page}")

    all_articles.extend(new_articles)

    if stop:
        print("Stopping: found older or duplicate article")
        break

    page += 1

driver.quit()

# ------------------ SAVE TO DB ------------------
if all_articles:
    insert_articles(all_articles)
    print(f"{len(all_articles)} articles added to DB from Financial Times.")
else:
    print("No new articles found to add.")
