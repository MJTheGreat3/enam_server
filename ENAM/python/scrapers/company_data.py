import os
import time
import gc
import psutil
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from threading import Lock

INSIDER_HEADERS = [
    "Stock", "Clause", "Name", "Type", "Amount", "Value", "Transaction", "Attachment", "Time"
]

ANNOUNCEMENTS_HEADERS = [
    "Stock", "Subject", "Announcement", "Attachment", "Time"
]

# === PATH CONFIGURATION ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

_db_lock = Lock()
_db_conn_pool = []

def init_db_pool():
    global _db_conn_pool
    with _db_lock:
        if not _db_conn_pool:
            for _ in range(5):  # Initial pool size
                conn = psycopg2.connect(
                    dbname="enam",
                    user="postgres",
                    password="mathew",
                    host="localhost",
                    port="5432"
                )
                _db_conn_pool.append(conn)

def get_db_connection():
    global _db_conn_pool
    with _db_lock:
        if not _db_conn_pool:
            init_db_pool()
        return _db_conn_pool.pop()

def return_db_connection(conn):
    global _db_conn_pool
    with _db_lock:
        _db_conn_pool.append(conn)

def close_all_connections():
    global _db_conn_pool
    with _db_lock:
        for conn in _db_conn_pool:
            try:
                conn.close()
            except:
                pass
        _db_conn_pool = []

def log_debug(message):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} {message}")

def load_portfolio_symbols(only_new=False):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if only_new:
                cur.execute("""
                    SELECT symbol FROM symbols 
                    WHERE status = TRUE AND last_scraped IS NULL
                """)
            else:
                cur.execute("SELECT symbol FROM symbols WHERE status = TRUE")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Database error fetching symbols: {str(e)[:200]}")
        return []
    finally:
        if conn:
            return_db_connection(conn)

def append_unique_rows(table, rows):
    if not rows:
        return
        
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            if table == "announcements":
                query = """
                    INSERT INTO announcements 
                    (stock, subject, announcement, attachment, time)
                    VALUES %s ON CONFLICT (stock, subject, time) DO NOTHING
                """
            elif table == "insider_trading":
                query = """
                    INSERT INTO insider_trading 
                    (stock, clause, name, type, amount, value, transaction, attachment, time)
                    VALUES %s ON CONFLICT (stock, name, transaction, time) DO NOTHING
                """
            else:
                raise ValueError(f"Unknown table: {table}")
            
            # Convert empty strings to None for PostgreSQL
            processed_rows = []
            for row in rows:
                processed_row = [
                    None if isinstance(x, str) and not x.strip() else x 
                    for x in row
                ]
                processed_rows.append(processed_row)
            
            execute_values(cur, query, processed_rows)
            conn.commit()
    except Exception as e:
        print(f"Database error inserting into {table}: {str(e)[:200]}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            return_db_connection(conn)

def check_system_resources():
    cpu_threshold = 80
    mem_threshold = 85
    wait_time = 5
    max_attempts = 5

    attempt = 0
    while attempt < max_attempts:
        cpu_usage = psutil.cpu_percent(interval=1)
        mem_usage = psutil.virtual_memory().percent
        if cpu_usage < cpu_threshold and mem_usage < mem_threshold:
            return
        print(f"[WARN] High usage - CPU: {cpu_usage}%, Mem: {mem_usage}% - GC Attempt {attempt+1}")
        gc.collect()
        attempt += 1
        time.sleep(wait_time)
    raise Exception("Resources too constrained after attempts.")

def convert_nse_datetime(raw):
    try:
        dt = datetime.strptime(raw.strip(), "%d-%b-%Y %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return raw

# === WebDriver ===
def create_driver():
    options = Options()
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-default-apps")
    options.add_argument("--incognito")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver

def log_error(company, error, driver=None):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    print(f"{timestamp} ERROR - {company}: {error}")
    
    if driver:
        print(f"{timestamp} HTML Snapshot for {company}:\n{'='*40}")
        print(driver.page_source)
        print(f"{'='*40}\n")

# === Scraper ===
def scrape_company_data(company):
    for attempt in range(3):
        driver = None
        try:
            print(f"Starting scrape for: {company}")
            # check_system_resources()
            driver = create_driver()
            driver.get(f"https://www.nseindia.com/get-quotes/equity?symbol={company}")
            wait = WebDriverWait(driver, 20)

            # ANNOUNCEMENTS
            try:
                ann_button = wait.until(EC.presence_of_element_located((By.ID, "announcements")))
                if not ann_button.is_displayed():
                    raise Exception("Announcements tab hidden or absent")

                driver.execute_script("arguments[0].scrollIntoView(true);", ann_button)
                wait.until(EC.element_to_be_clickable((By.ID, "announcements")))
                driver.execute_script("arguments[0].click();", ann_button)
                time.sleep(1)

                try:
                    ten_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-val="6M"]')))
                    driver.execute_script("arguments[0].click();", ten_button)
                    time.sleep(1)
                except Exception:
                    print(f"[{company}] Could not select 6M filter")

                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#corpAnnouncementTable tbody')))
                readmores = driver.find_elements(By.CSS_SELECTOR, 'a.readMore')
                for link in readmores:
                    try:
                        driver.execute_script("arguments[0].click();", link)
                        time.sleep(0.2)
                    except:
                        continue

                soup = BeautifulSoup(driver.page_source, "html.parser")
                div = soup.find('div', id="corpAnnouncementTable")
                anns = []
                if div and div.find('tbody'):
                    for row in div.find('tbody').find_all('tr'):
                        tds = row.find_all("td")
                        if len(tds) < 4:
                            continue
                        ann = [company]
                        ann.append(tds[0].get_text(strip=True))
                        span = tds[1].find("span")
                        ann.append(span.get_text(strip=True) if span else tds[1].get_text(strip=True))
                        a_tag = tds[2].find("a")
                        ann.append(a_tag.get("href") if a_tag else None)
                        for d in tds[3].find_all("div"):
                            d.extract()
                        time_val = tds[3].get_text(strip=True)
                        ann.append(convert_nse_datetime(time_val))
                        anns.append(ann)

                    if anns:
                        append_unique_rows("announcements", anns)
                        print(f"[{company}] Announcements extracted: {len(anns)} records")
                    else:
                        print(f"[{company}] No announcements found")
                else:
                    print(f"[{company}] Announcements table missing")
            except Exception as e:
                print(f"[{company}] Announcements error: {str(e)[:150]}")

            # INSIDER TRADING
            try:
                it_button = wait.until(EC.presence_of_element_located((By.ID, "insiderTrading")))
                if not it_button.is_displayed():
                    raise Exception("Insider Trading tab hidden or absent")

                driver.execute_script("arguments[0].scrollIntoView(true);", it_button)
                wait.until(EC.element_to_be_clickable((By.ID, "insiderTrading")))
                driver.execute_script("arguments[0].click();", it_button)
                time.sleep(2)

                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#corpInsiderTradingTable tbody')))
                soup = BeautifulSoup(driver.page_source, "html.parser")
                div = soup.find('div', id="corpInsiderTradingTable")
                its = []
                if div and div.find('tbody'):
                    for row in div.find('tbody').find_all('tr'):
                        tds = row.find_all('td')
                        if len(tds) != 8:
                            continue
                        it = [company]
                        for n in range(8):
                            if n == 6 and tds[n].find('a'):
                                it.append(tds[n].find('a').get("href"))
                            elif n == 7:
                                it.append(convert_nse_datetime(tds[n].get_text(strip=True)))
                            else:
                                it.append(tds[n].get_text(strip=True))
                        its.append(it)

                    if its:
                        append_unique_rows("insider_trading", its)
                        print(f"[{company}] Insider Trading extracted: {len(its)} records")
                    else:
                        print(f"[{company}] No insider trading data found")
                else:
                    print(f"[{company}] Insider Trading table missing")
            except Exception as e:
                print(f"[{company}] Insider Trading error: {str(e)[:150]}")
            return
        except Exception as e:
            print(f"{company} attempt {attempt+1}/3 failed: {str(e)[:100]}")
            time.sleep(2 ** attempt)
        finally:
            if driver:
                driver.quit()

# === Runner ===
def run_company_scrapers(only_new=False):
    # Initialize connection pool on import
    init_db_pool()

    start_time = time.time()
    companies = load_portfolio_symbols(only_new)
    if not companies:
        print("[WARN] No active companies found in symbols table")
        return

    # Limit workers based on system resources
    cpu_count = os.cpu_count() or 1
    max_workers = min(1, cpu_count, len(companies))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape_company_data, c): c for c in companies}
        for future in concurrent.futures.as_completed(futures):
            company = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"[ERROR] Thread failed for {company}: {str(e)[:100]}")

    print(f"Company scraping completed in {time.time() - start_time:.2f} seconds")
    close_all_connections()