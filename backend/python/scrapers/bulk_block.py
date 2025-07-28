import os
import time
import re
import requests
import traceback
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
import concurrent.futures
import psycopg2
from psycopg2.extras import execute_values
from .common import log_debug, check_system_resources

def get_db_connection():
    return psycopg2.connect(
        dbname="enam",
        user="postgres",
        password="mathew",
        host="localhost",
        port="5432"
    )

def insert_into_table(table, rows):
    if not rows:
        return
    with get_db_connection() as conn, conn.cursor() as cur:
        placeholders = ','.join(['%s'] * len(rows[0]))
        query = f"""
        INSERT INTO {table} VALUES %s
        ON CONFLICT DO NOTHING;
        """
        execute_values(cur, query, rows)
        conn.commit()

def create_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver

def scrape_bse_bulk():
    for attempt in range(3):
        try:
            check_system_resources()
            driver = create_driver()
            driver.get("https://www.bseindia.com/markets/equity/EQReports/bulk_deals.aspx")
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[name*='notedate']")))
            soup = BeautifulSoup(driver.page_source, "html.parser")
            date_string = soup.find('span', attrs={'name': re.compile(r'notedate')}).get_text(strip=True)
            deal_date = datetime.strptime(date_string.split(':')[-1].strip(), "%d %b %Y").date()

            table = soup.find('table', attrs={'name': re.compile(r'bulkdeals')})
            body = table.find('tbody')
            rows = []
            for row in body.find_all('tr'):
                cells = [c.get_text(strip=True) for c in row.find_all('td')]
                if len(cells) >= 6:
                    deal_type = "BUY" if cells[4] == "B" else "SELL"
                    rows.append([
                        "BSE", deal_date, cells[0], cells[2], deal_type, cells[5], cells[6]
                    ])
            insert_into_table("bulk_deals", rows)
            print(f"BSE Bulk Deals extracted ({deal_date})")
            return
        except Exception as e:
            print(f"BSE Bulk attempt {attempt+1}/3 failed: {str(e)[:100]}")
            time.sleep(2 ** attempt)
        finally:
            try: driver.quit()
            except: pass

def scrape_bse_block():
    for attempt in range(3):
        try:
            check_system_resources()
            driver = create_driver()
            driver.get("https://www.bseindia.com/markets/equity/EQReports/block_deals.aspx")
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span[name*='note']")))
            soup = BeautifulSoup(driver.page_source, "html.parser")
            date_string = soup.find('span', attrs={'name': re.compile(r'note')}).get_text(strip=True)
            deal_date = datetime.strptime(date_string.split(':')[-1].strip(), "%d %b %Y").date()

            table = soup.find('table', attrs={'name': re.compile(r'block')})
            body = table.find('tbody')
            rows = []
            for row in body.find_all('tr'):
                cells = [c.get_text(strip=True) for c in row.find_all('td')]
                if len(cells) >= 6:
                    deal_type = "Buy" if cells[4] == "B" else "Sell"
                    rows.append([
                        "BSE", deal_date, cells[0], cells[2], deal_type, cells[5], cells[6]
                    ])
            insert_into_table("block_deals", rows)
            print(f"BSE Block Deals extracted ({deal_date})")
            return
        except Exception as e:
            print(f"BSE Block attempt {attempt+1}/3 failed: {str(e)[:100]}")
            time.sleep(2 ** attempt)
        finally:
            try: driver.quit()
            except: pass

def scrape_nse_bulk():
    for attempt in range(3):
        try:
            check_system_resources()
            today = datetime.today()
            params = {
                'optionType': 'bulk_deals',
                'from': (today - timedelta(days=30)).strftime("%d-%m-%Y"),
                'to': today.strftime("%d-%m-%Y"),
            }
            headers = {
                'referer': 'https://www.nseindia.com/report-detail/display-bulk-and-block-deals',
                'user-agent': 'Mozilla/5.0'
            }
            session = requests.Session()
            session.get("https://www.nseindia.com", headers=headers)
            response = session.get("https://www.nseindia.com/api/historicalOR/bulk-block-short-deals", headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            rows = []
            for r in data.get('data', []):
                deal_date = datetime.strptime(r['BD_DT_DATE'], "%d-%b-%Y").date()
                rows.append([
                    "NSE", deal_date, r['BD_SYMBOL'], r['BD_CLIENT_NAME'], r['BD_BUY_SELL'], r['BD_QTY_TRD'], r['BD_TP_WATP']
                ])
            insert_into_table("bulk_deals", rows)
            print("NSE Bulk Deals extracted")
            return
        except Exception as e:
            print(f"NSE Bulk attempt {attempt+1}/3 failed: {str(e)[:100]}")
            time.sleep(2 ** attempt)

def scrape_nse_block():
    for attempt in range(3):
        try:
            check_system_resources()
            today = datetime.today()
            params = {
                'optionType': 'block_deals',
                'from': (today - timedelta(days=30)).strftime("%d-%m-%Y"),
                'to': today.strftime("%d-%m-%Y"),
            }
            headers = {
                'referer': 'https://www.nseindia.com/report-detail/display-bulk-and-block-deals',
                'user-agent': 'Mozilla/5.0'
            }
            session = requests.Session()
            session.get("https://www.nseindia.com", headers=headers)
            response = session.get("https://www.nseindia.com/api/historicalOR/bulk-block-short-deals", headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            rows = []
            for r in data.get('data', []):
                deal_date = datetime.strptime(r['BD_DT_DATE'], "%d-%b-%Y").date()
                rows.append([
                    "NSE", deal_date, r['BD_SYMBOL'], r['BD_CLIENT_NAME'], r['BD_BUY_SELL'], r['BD_QTY_TRD'], r['BD_TP_WATP']
                ])
            insert_into_table("block_deals", rows)
            print("NSE Block Deals extracted")
            return
        except Exception as e:
            print(f"NSE Block attempt {attempt+1}/3 failed: {str(e)[:100]}")
            time.sleep(2 ** attempt)

def run_bulk_block_scrapers():
    start_time = time.time()
    tasks = [scrape_bse_bulk, scrape_bse_block, scrape_nse_bulk, scrape_nse_block]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(fn) for fn in tasks]
        for future in concurrent.futures.as_completed(futures):
            try: future.result()
            except: pass
    print(f"Bulk/Block scraping completed in {time.time()-start_time:.2f} seconds")
