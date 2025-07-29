import os
import subprocess
from config import Config
from services.file_service import set_last_updated
from python.scrapers import company_data
import threading

def run_cleaner():
    """Run the cleaner script"""
    logs = []
    cleaner_script = os.path.join('cleaner.py')
    
    if os.path.exists(os.path.join('python', 'cleaner.py')):
        logs.append("[INFO] Running cleaner.py...")
        try:
            result = subprocess.run(
                ['python', 'cleaner.py'],
                cwd='python',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            logs.append("[SUCCESS] cleaner.py completed.")
            if result.stdout:
                logs.append(result.stdout)
            if result.stderr:
                logs.append(f"[STDERR] {result.stderr}")
        except subprocess.CalledProcessError as e:
            logs.append(f"[ERROR] cleaner.py failed with code {e.returncode}")
            logs.append(e.stdout or "")
            logs.append(f"[STDERR] {e.stderr or ''}")
    else:
        logs.append("[WARNING] cleaner.py not found.")
    
    return logs

def run_python_script(script_path):
    """Run a Python script and return logs"""
    logs = []
    script_name = os.path.basename(script_path)
    
    if not os.path.exists(script_path):
        logs.append(f"[ERROR] Script not found: {script_path}")
        return logs

    # Get appropriate lock
    lock = Config.NEWS_SCRIPT_LOCKS.get(script_name, Config.SCRIPT_LOCK)
    
    with lock:
        logs.append(f"[INFO] Running: {script_path}")
        try:
            script_dir = os.path.dirname(script_path)
            result = subprocess.run(
                ['python', script_name],
                cwd=script_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            logs.append(f"[SUCCESS] {script_path} completed.")
            if result.stdout:
                logs.append(result.stdout)
            if result.stderr:
                logs.append(f"[STDERR] {result.stderr}")
        except subprocess.CalledProcessError as e:
            logs.append(f"[ERROR] {script_path} failed with code {e.returncode}.")
            logs.append(e.stdout or "")
            logs.append(f"[STDERR] {e.stderr or ''}")
    
    return logs

def run_all_data_scripts():
    """Run all data collection scripts"""
    logs = []
    
    # Run bulk/block scrapers
    logs.append("[INFO] Running bulk/block scrapers...")
    try:
        result = subprocess.run(
            ['python', 'scraper.py', 'all'],
            cwd='python',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        logs.append("[SUCCESS] Bulk/Block scraping completed.")
        logs.append(result.stdout)
        if result.stderr:
            logs.append(f"[STDERR] {result.stderr}")
    except Exception as e:
        logs.append(f"[ERROR] Bulk/Block scraping failed: {str(e)}")

    # Run individual data scripts
    for script in filter(os.path.exists, Config.DATA_SCRIPTS):
        logs.extend(run_python_script(script))

    set_last_updated(Config.LAST_UPDATED_DATA_FILE)
    return logs

def run_all_news_scripts():
    """Run all news collection scripts"""
    logs = []
    news_folder = os.path.join('python', 'news')
    
    scripts = [
        os.path.join(news_folder, s) for s in Config.NEWS_SCRIPTS_WHITELIST
        if os.path.exists(os.path.join(news_folder, s))
    ]

    if not scripts:
        logs.append("[WARNING] No news scripts found to run.")
        return logs

    logs.append(f"[INFO] Found {len(scripts)} news scripts to run.")

    for script in scripts:
        logs.append(f"[INFO] Running news script: {script}")
        logs.extend(run_python_script(script))

        logs.append("[INFO] Running cleaner after news script.")
        logs.extend(run_cleaner())

    set_last_updated(Config.LAST_UPDATED_NEWS_FILE)
    logs.append("[INFO] All news scripts (and cleaning) complete.")
    return logs

def run_company_scrapers_async():
    """Run company scrapers in background thread"""
    def target():
        try:
            logs = []
            logs.append("[INFO] Running company data scrapers...")
            company_data.run_company_scrapers()
            logs.append("[SUCCESS] Company data scraping completed")
            print("\n".join(logs))
        except Exception as e:
            print(f"[ERROR] Company scraping failed: {str(e)}")

    threading.Thread(target=target, daemon=True).start()