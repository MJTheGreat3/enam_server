from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import threading
from backend.utils import get_last_updated
from backend.tasks import run_all_data_scripts, run_all_news_scripts

scheduler = BackgroundScheduler()
scheduler_enabled = {"data": True, "news": True}
DATA_JOB_ID = "data_refresh_job"
NEWS_JOB_ID = "news_refresh_job"

def schedule_jobs():
    if scheduler_enabled["data"]:
        scheduler.add_job(run_all_data_scripts, 'interval', minutes=180, id=DATA_JOB_ID, replace_existing=True)
    if scheduler_enabled["news"]:
        scheduler.add_job(run_all_news_scripts, 'interval', minutes=10, id=NEWS_JOB_ID, replace_existing=True)

def run_scheduled_jobs():
    now = datetime.datetime.now()
    data_ts = get_last_updated("data")
    news_ts = get_last_updated("news")

    needs_data = not data_ts or (now - datetime.datetime.strptime(data_ts, "%Y-%m-%d %H:%M:%S")).total_seconds() > 10800
    needs_news = not news_ts or (now - datetime.datetime.strptime(news_ts, "%Y-%m-%d %H:%M:%S")).total_seconds() > 600

    if needs_data or needs_news:
        def job():
            if needs_data:
                run_all_data_scripts()
            if needs_news:
                run_all_news_scripts()
        threading.Thread(target=job, daemon=True).start()
