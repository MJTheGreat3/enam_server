import os
import time
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
import re

# === DB Connection Pool ===
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

def insert_into_table(table, rows):
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

def get_active_symbols(only_new=False):
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

# === WebDriver ===
def create_driver():
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    })
    driver.set_page_load_timeout(45)
    driver.implicitly_wait(10)
    return driver

def log_error(company, error, driver=None):
    error_dir = "scraper_errors"
    os.makedirs(error_dir, exist_ok=True)
    
    with open(f"{error_dir}/errors.log", "a") as f:
        f.write(f"{datetime.now()} - {company}: {error}\n")
    
    if driver:
        with open(f"{error_dir}/{company}_error.html", "w") as f:
            f.write(driver.page_source)

# === Time Conversion ===
def convert_nse_datetime(time_str):
    """Convert NSE time format to PostgreSQL timestamp"""
    try:
        # Handle cases like "05-Jul-2025 17:41:47"
        if re.match(r'\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}', time_str):
            return datetime.strptime(time_str, '%d-%b-%Y %H:%M:%S')
        
        # Handle other formats if needed
        return datetime.strptime(time_str, '%d-%b-%Y')
    except Exception:
        return None

def clean_time_element(element):
    """Remove inner tables and extract only time text"""
    # Remove all child elements (tables, divs, etc.)
    for child in element.find_all():
        child.decompose()
    return element.get_text(strip=True)

# === Scraper ===
def scrape_company_data(company):
    for attempt in range(3):
        driver = None
        try:
            print(f"Attempt {attempt+1} for {company}")
            driver = create_driver()
            
            try:
                # Step 1: Load company page
                driver.get(f"https://www.nseindia.com/get-quotes/equity?symbol={company}")
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#sidebar"))
                )
                
                if "captcha" in driver.page_source.lower():
                    raise Exception("CAPTCHA encountered")

                # --- Announcements ---
                announcements = scrape_announcements(driver, company)
                if announcements:
                    insert_into_table("announcements", announcements)

                # --- Insider Trading ---
                insider_trades = scrape_insider_trading(driver, company)
                if insider_trades:
                    insert_into_table("insider_trading", insider_trades)

                # Update last_scraped timestamp if successful
                if announcements or insider_trades:
                    update_last_scraped(company)
                
                return  # Success
                
            except Exception as e:
                print(f"Page interaction failed: {str(e)}")
                raise

        except Exception as e:
            print(f"{company} attempt {attempt+1} failed: {str(e)}")
            if attempt == 2:  # Final attempt
                log_error(company, str(e), driver)
            time.sleep(5 ** (attempt + 1))  # Exponential backoff
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

def scrape_announcements(driver, company):
    announcements = []
    try:
        # Step 2: Find and click announcements tab
        ann_button = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "announcements"))
        )
        driver.execute_script("arguments[0].click();", ann_button)
        
        # Step 3: Wait for table and expand all "Read More" links
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'corpAnnouncementTable'))
        )
        
        # Expand all "Read More" links
        for link in driver.find_elements(By.CSS_SELECTOR, 'a.readMore'):
            try:
                driver.execute_script("arguments[0].click();", link)
                time.sleep(0.3)
            except:
                continue
                
        time.sleep(2)  # Allow content to expand

        # Parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        table_div = soup.find('div', id="corpAnnouncementTable")
        
        if table_div and table_div.find('tbody'):
            # Step 4: Process each row
            for row in table_div.find('tbody').find_all('tr'):
                tds = row.find_all("td")
                if len(tds) < 4:
                    continue
                    
                # Step 4a: Extract subject and announcement text
                subject = tds[0].get_text(strip=True)
                announcement = tds[1].get_text(strip=True)
                
                # Step 5: Extract attachment href
                attachment = None
                if tds[2].find("a"):
                    attachment = tds[2].find("a").get("href", "")
                    if attachment and not attachment.startswith('http'):
                        attachment = f"https://www.nseindia.com{attachment}"
                        
                # Step 6: Clean time element and parse
                time_element = tds[3]
                time_str = clean_time_element(time_element)
                time_val = convert_nse_datetime(time_str)
                
                if subject and announcement and time_val:  # Basic validation
                    announcements.append([
                        company, subject, announcement, 
                        attachment, time_val
                    ])
        
        if announcements:
            print(f"[{company}] Announcements: {len(announcements)} records")
        else:
            print(f"[{company}] No announcement data found")
            
    except Exception as e:
        print(f"[{company}] Announcements error: {str(e)}")
    
    return announcements

def scrape_insider_trading(driver, company):
    insider_trades = []
    try:
        # Step 7: Click insider trading tab
        it_button = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "insiderTrading")))
        driver.execute_script("arguments[0].click();", it_button)
        
        # Step 8: Wait for table to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'corpInsiderTradingTable')))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        table_div = soup.find('div', id="corpInsiderTradingTable")
        
        if table_div and table_div.find('tbody'):
            # Process each row
            for row in table_div.find('tbody').find_all('tr'):
                tds = row.find_all('td')
                if len(tds) != 8:
                    continue
                    
                # Extract text from first 6 columns
                clause = tds[0].get_text(strip=True)
                name = tds[1].get_text(strip=True)
                type_ = tds[2].get_text(strip=True)
                amount = tds[3].get_text(strip=True)
                value = tds[4].get_text(strip=True)
                transaction = tds[5].get_text(strip=True)
                
                # Step 8a: Extract attachment href from 7th column
                attachment = None
                if tds[6].find('a'):
                    attachment = tds[6].find('a').get("href", "")
                    if attachment and not attachment.startswith('http'):
                        attachment = f"https://www.nseindia.com{attachment}"
                
                # Step 8b: Clean time element and parse
                time_element = tds[7]
                time_str = clean_time_element(time_element)
                time_val = convert_nse_datetime(time_str)
                
                if name and time_val:  # Basic validation
                    insider_trades.append([
                        company, clause, name, type_, amount, 
                        value, transaction, attachment, time_val
                    ])
        
        if insider_trades:
            print(f"[{company}] Insider Trading: {len(insider_trades)} records")
        else:
            print(f"[{company}] No insider data found")
            
    except Exception as e:
        print(f"[{company}] Insider Trading error: {str(e)}")
    
    return insider_trades

def update_last_scraped(symbol):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE symbols 
                SET last_scraped = NOW() 
                WHERE symbol = %s
            """, (symbol,))
            conn.commit()
    except Exception as e:
        print(f"Error updating last_scraped for {symbol}: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            return_db_connection(conn)

# === Runner ===
def run_company_scrapers(only_new=False):
    start_time = time.time()
    companies = get_active_symbols(only_new)
    if not companies:
        print("[WARN] No active companies found in symbols table")
        return

    # Limit workers based on system resources
    cpu_count = os.cpu_count() or 1
    max_workers = min(4, cpu_count, len(companies))
    
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

# Initialize connection pool on import
init_db_pool()