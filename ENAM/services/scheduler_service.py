import datetime
import threading
from config import Config
from services.file_service import get_last_updated
from services.data_service import run_all_data_scripts, run_all_news_scripts

def run_scheduled_jobs():
    """Check and run initial data/news updates if needed"""
    print("[INFO] Checking whether initial data and news updates are needed...")
    now = datetime.datetime.now()

    # Check if data needs updating
    last_data_str = get_last_updated(Config.LAST_UPDATED_DATA_FILE)
    data_needs_update = (
        not last_data_str or 
        (now - datetime.datetime.strptime(last_data_str, "%Y-%m-%d %H:%M:%S")).total_seconds() 
        > Config.DATA_UPDATE_INTERVAL
    )

    # Check if news needs updating
    last_news_str = get_last_updated(Config.LAST_UPDATED_NEWS_FILE)
    news_needs_update = (
        not last_news_str or 
        (now - datetime.datetime.strptime(last_news_str, "%Y-%m-%d %H:%M:%S")).total_seconds() 
        > Config.NEWS_UPDATE_INTERVAL
    )

    def background_job():
        """Background job to run updates"""
        logs = []
        if data_needs_update:
            logs.extend(run_all_data_scripts())
        if news_needs_update:
            logs.extend(run_all_news_scripts())
        print("[INITIAL SCHEDULED RUN LOGS]")
        print("\n".join(logs))

    if data_needs_update or news_needs_update:
        threading.Thread(target=background_job, daemon=True).start()
    else:
        print("[INFO] No initial refresh needed.")