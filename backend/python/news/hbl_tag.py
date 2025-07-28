import time
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pyahocorasick

# ----------------------------------------------------------------------------
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
def process_tagging():
    safe_print("[TAGGING] Starting tagging process...")

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

    try:
        # Load symbols
        safe_print("[TAGGING] Loading symbols and aliases from DB...")
        with conn.cursor() as cur:
            cur.execute("SELECT Tag_ID, Alias FROM symbols WHERE Status IS TRUE")
            symbols_data = cur.fetchall()
        safe_print(f"[TAGGING] Loaded {len(symbols_data)} symbols.")

        # Build Aho-Corasick
        A = pyahocorasick.Automaton()
        alias_to_tagid = {}
        for tag_id, alias_field in symbols_data:
            if not alias_field:
                continue
            aliases = [a.strip().lower() for a in alias_field.split('|') if a.strip()]
            for alias in aliases:
                A.add_word(alias, (alias, tag_id))
                alias_to_tagid.setdefault(alias, set()).add(tag_id)
        A.make_automaton()
        safe_print(f"[TAGGING] Aho-Corasick built with {len(alias_to_tagid)} distinct aliases.")

        # Find untagged news
        safe_print("[TAGGING] Querying untagged news records...")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT n.id, n.headline, n.link
                FROM news n
                LEFT JOIN tagging t ON n.id = t.news_id
                WHERE t.news_id IS NULL
            """)
            untagged_news = cur.fetchall()
        safe_print(f"[TAGGING] Found {len(untagged_news)} untagged news articles.")

        if not untagged_news:
            safe_print("[TAGGING] Nothing to process. Exiting.")
            conn.close()
            return

        # Setup Selenium
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        driver = webdriver.Chrome(options=chrome_options)

        new_mappings = []

        for news_id, headline, link in untagged_news:
            safe_print(f"\n[TAGGING] Processing News ID: {news_id}")
            text_corpus = (headline or "").lower() + " "

            try:
                driver.get(link)
                time.sleep(3)
                soup = BeautifulSoup(driver.page_source, 'html.parser')

                sub_title_tag = soup.find('h2', class_='sub-title')
                if sub_title_tag and sub_title_tag.text:
                    text_corpus += sub_title_tag.text.lower().strip() + " "

                article_main = soup.find('div', class_='article-main')
                if article_main:
                    for p_tag in article_main.find_all('p'):
                        if p_tag and p_tag.text:
                            text_corpus += p_tag.text.lower().strip() + " "

                safe_print(f"[TAGGING] Fetched article content for news_id {news_id}")

            except Exception as e:
                safe_print(f"[ERROR] Could not load or parse article {link}: {e}")
                continue

            matched_tag_ids = set()
            for end_idx, (alias, tag_id) in A.iter(text_corpus):
                matched_tag_ids.add(tag_id)

            if matched_tag_ids:
                safe_print(f"[TAGGING] Matches found: {matched_tag_ids}")
                for tag_id in matched_tag_ids:
                    new_mappings.append((tag_id, news_id))
            else:
                safe_print(f"[TAGGING] No matches for news_id {news_id}")

        driver.quit()

        # Insert
        if new_mappings:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO tagging (Tag_ID, news_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    new_mappings
                )
            conn.commit()
            safe_print(f"[TAGGING] Inserted {len(new_mappings)} new tagging mappings.")
        else:
            safe_print("[TAGGING] No new tagging mappings to insert.")

        conn.close()
        safe_print("[TAGGING] Tagging process complete.")

    except Exception as e:
        safe_print("[ERROR] Unexpected error in tagging process:", e)
        conn.close()

# ----------------------------------------------------------------------------
if __name__ == "__main__":
    process_tagging()
