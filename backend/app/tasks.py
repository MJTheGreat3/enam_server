import os
import subprocess
import threading
from backend.utils import get_db_connection, set_last_updated
from python.scrapers import company_data

TEMP_LIST = set()

SCRIPT_LOCK = threading.Lock()
NEWS_SCRIPTS_WHITELIST = [
    'business_line.py', 'business_std.py', 'cnbctv_18.py',
    'econ_times.py', 'fin_exp.py', 'ft.py',
    'investing.py', 'money_control.py', 'ndtvprofit.py'
]
NEWS_SCRIPT_LOCKS = {script: threading.Lock() for script in NEWS_SCRIPTS_WHITELIST}


def run_python_script(script_path):
    logs = []
    script_name = os.path.basename(script_path)
    lock = NEWS_SCRIPT_LOCKS.get(script_name, SCRIPT_LOCK)
    with lock:
        try:
            result = subprocess.run(
                ['python', script_name],
                cwd=os.path.dirname(script_path),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            logs.append(result.stdout)
        except subprocess.CalledProcessError as e:
            logs.append(f"Error: {e.stderr}")
    return logs


def run_cleaner():
    return run_python_script('python/cleaner.py')


def run_all_data_scripts():
    logs = []
    logs.extend(run_python_script('python/scraper.py all'))
    for script in ['python/mutual_funds.py', 'python/corp_actions.py', 'python/volume_reports.py']:
        if os.path.exists(script):
            logs.extend(run_python_script(script))
    set_last_updated("data")
    return logs


def run_all_news_scripts():
    logs = []
    news_folder = 'python/news'
    for script in NEWS_SCRIPTS_WHITELIST:
        path = os.path.join(news_folder, script)
        if os.path.exists(path):
            logs.extend(run_python_script(path))
            logs.extend(run_cleaner())
    set_last_updated("news")
    return logs


def run_company_scrapers_async():
    def scrape():
        try:
            company_data.run_company_scrapers()
        except Exception as e:
            print(f"[ERROR] Company scraper failed: {e}")
    threading.Thread(target=scrape, daemon=True).start()
