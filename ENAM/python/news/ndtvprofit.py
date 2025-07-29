import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="enam",
        user="postgres",
        password="mathew"
    )

SOURCE_NAME = "NDTV Profit"
BASE_URL = "https://www.ndtvprofit.com"
CUTOFF_TIME = datetime.now() - timedelta(hours=24)
ALLOWED_CATEGORIES = {"markets", "economy-finance", "ipos", "research-reports"}

# === SETUP BROWSER ===
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
driver = webdriver.Chrome(options=options)

# === FETCH EXISTING HEADLINES FROM DB ===
def load_existing_headlines():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SET client_encoding TO 'UTF8';")
        cur.execute("SELECT headline FROM news WHERE source = %s;", (SOURCE_NAME,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return set(row[0] for row in rows)
    except Exception as e:
        print(f"[ERROR] Failed to load headlines: {e}")
        return set()

existing_headlines = load_existing_headlines()
session_headlines = set()

# === TIME PARSER ===
def parse_timestamp(time_str):
    try:
        time_str = time_str.strip().replace(" IST", "")
        return datetime.strptime(time_str, "%d %b %Y, %I:%M %p")
    except:
        return None

# === SCRAPING FUNCTION ===
def extract_articles():
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    containers = soup.find_all("div", class_=lambda x: x and "image-and-title-m__story-details" in x)
    results = []
    stop_flag = False

    for div in containers:
        try:
            a_tag = div.find("a", href=True)
            if not a_tag:
                continue

            h2_tag = a_tag.find("h2")
            if not h2_tag:
                continue

            headline = h2_tag.text.strip()
            link = BASE_URL + a_tag["href"]
            category = a_tag["href"].split("/")[1]
            time_div = div.find("div", class_=lambda x: x and "story-time" in x)
            time_str = time_div.text.strip() if time_div else ""
            dt = parse_timestamp(time_str)

            if not dt or dt < CUTOFF_TIME:
                stop_flag = True
                break
            if headline in existing_headlines or headline in session_headlines:
                stop_flag = True
                break
            if category not in ALLOWED_CATEGORIES:
                continue

            results.append((SOURCE_NAME, headline, link, category, dt.isoformat()))
            session_headlines.add(headline)
        except:
            continue

    return results, stop_flag

# === MAIN SCRAPING LOOP ===
driver.get("https://www.ndtvprofit.com/the-latest?src=topnav")
time.sleep(5)

all_articles = []
should_stop = False

while not should_stop:
    batch, should_stop = extract_articles()
    all_articles.extend(batch)

    if not should_stop:
        buttons = driver.find_elements(By.XPATH, '//button[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "more stories")]')
        if not buttons:
            break
        try:
            time.sleep(2)
            driver.execute_script("arguments[0].click();", buttons[0])
            time.sleep(4)
        except:
            break

driver.quit()

# === SAVE TO DATABASE ===
if all_articles:
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SET client_encoding TO 'UTF8';")

        insert_query = """
            INSERT INTO news (source, headline, link, category, time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """
        cur.executemany(insert_query, all_articles)
        conn.commit()
        cur.close()
        conn.close()
        print(f"[INFO] Inserted {len(all_articles)} new articles into DB.")
    except Exception as e:
        print(f"[ERROR] Failed to insert into DB: {e}")
