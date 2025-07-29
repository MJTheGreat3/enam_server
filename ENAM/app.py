from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import threading
import datetime
import os

from config import Config
from routes import main_bp, api_bp
from services import run_scheduled_jobs, run_all_data_scripts, run_all_news_scripts

def create_app():
    app = Flask(
        __name__,
        template_folder='frontend/templates',
        static_folder='frontend/static',
        static_url_path='/static'
    )
    
    # Load configuration
    app.config.from_object(Config)
    
    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    
    return app

def setup_scheduler():
    """Setup and start the background scheduler"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_all_data_scripts, 'interval', minutes=180)
    scheduler.add_job(run_all_news_scripts, 'interval', minutes=10)
    scheduler.start()
    print("[INFO] Scheduler started.")
    return scheduler

if __name__ == '__main__':
    app = create_app()
    scheduler = setup_scheduler()
    run_scheduled_jobs()
    app.run(host='0.0.0.0', port=8000, debug=True)