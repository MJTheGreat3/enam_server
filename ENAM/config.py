import os
import threading

class Config:
    """Application configuration class"""
    
    # Base paths
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PORTFOLIO_FILE = os.path.join(SCRIPT_DIR, 'user_portfolio.csv')
    LAST_UPDATED_DATA_FILE = os.path.join(SCRIPT_DIR, 'last_updated_data.txt')
    LAST_UPDATED_NEWS_FILE = os.path.join(SCRIPT_DIR, 'last_updated_news.txt')
    
    # News scripts whitelist
    NEWS_SCRIPTS_WHITELIST = [
        'business_line.py', 'business_std.py', 'cnbctv_18.py',
        'econ_times.py', 'fin_exp.py', 'ft.py',
        'investing.py', 'money_control.py', 'ndtvprofit.py'
    ]
    
    # Data scripts list
    DATA_SCRIPTS = [
        'python/mutual_funds.py',
        'python/corp_actions.py',
        'python/volume_reports.py'
    ]
    
    # Threading locks
    SCRIPT_LOCK = threading.Lock()
    NEWS_SCRIPT_LOCKS = {script: threading.Lock() for script in NEWS_SCRIPTS_WHITELIST}
    
    # Update intervals (in seconds)
    DATA_UPDATE_INTERVAL = 3 * 60 * 60  # 3 hours
    NEWS_UPDATE_INTERVAL = 10 * 60      # 10 minutes
