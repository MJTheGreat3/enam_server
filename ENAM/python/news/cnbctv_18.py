import time
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import psycopg2

# ----------------------------------------------------------------------------
# Constants
SOURCE = "CNBC TV 18"
URL = "https://www.cnbctv18.com/latest-news/"
ALLOWED_CATEGORIES = {"market", "stock", "business", "economy"}

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
def parse_cnbc_time(date_str):
    try:
        dt = datetime.strptime(date_str.strip(), "%b %d, %Y %I:%M %p")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

# ----------------------------------------------------------------------------
def extract_articles(soup, existing_links):
    articles = soup.find_all("article", class_="story-item")
    extracted = []
    stop_flag = False

    for article in articles:
        try:
            category_tag = article.find("span", class_="story-cat")
            category = category_tag.text.strip().lower() if category_tag else ""
            if category not in ALLOWED_CATEGORIES:
                continue

            title_tag = article.find("h2", class_="story-title")
            title = title_tag.text.strip() if title_tag else ""
            link_tag = title_tag.find_parent("a") if title_tag else None
            link = link_tag["href"] if link_tag and link_tag.has_attr("href") else ""

            time_tag = article.find("time")
            published_str = time_tag.text.strip() if time_tag else ""
            published_dt = parse_cnbc_time(published_str)

            if not published_dt:
                continue

            if published_dt.date() != datetime.now().date():
                stop_flag = True
                break

            if link in existing_links:
                stop_flag = True
                break

            extracted.append({
                "Source": SOURCE,
                "Headline": title,
                "Link": link,
                "Category": category,
                "Time": published_dt.strftime("%Y-%m-%d %H:%M:%S")
            })

            safe_print("[SCRAPER][NEW]", category, "|", title)

        except Exception as e:
            continue

    return extracted, stop_flag

# ----------------------------------------------------------------------------
def scroll_and_scrape():
    safe_print("[SCRAPER] Starting CNBC TV18 Scraper")

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

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--log-level=3")
    driver = webdriver.Chrome(options=options)

    driver.get(URL)
    time.sleep(3)

    all_articles = []
    stop = False
    last_height = driver.execute_script("return document.body.scrollHeight")
    max_scrolls = 50

    for _ in range(max_scrolls):
        soup = BeautifulSoup(driver.page_source, "html.parser")
        new_articles, stop = extract_articles(soup, existing_links)
        all_articles.extend(new_articles)

        if stop:
            break

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    driver.quit()

    if all_articles:
        insert_articles_to_db(conn, all_articles)
        safe_print(f"[SCRAPER][SAVE] {len(all_articles)} articles added from CNBC TV18.")
    else:
        safe_print("[SCRAPER] 0 articles added from CNBC TV18.")

    conn.close()
    safe_print("[SCRAPER] Done.")

# ----------------------------------------------------------------------------
if __name__ == "__main__":
    scroll_and_scrape()
