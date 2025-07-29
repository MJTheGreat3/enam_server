import os
from datetime import timedelta

class Config:
    """Base configuration class"""
    
    # Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DEBUG = False
    TESTING = False
    
    # Database Configuration
    DB_CONFIG = {
        "dbname": os.environ.get('DB_NAME', "enam"),
        "user": os.environ.get('DB_USER', "postgres"),
        "password": os.environ.get('DB_PASSWORD', "mathew"),
        "host": os.environ.get('DB_HOST', "localhost"),
        "port": os.environ.get('DB_PORT', "5432")
    }
    
    # Scheduler Configuration
    SCHEDULER_CONFIG = {
        'data_refresh_interval': int(os.environ.get('DATA_REFRESH_INTERVAL', 180)),  # minutes
        'news_refresh_interval': int(os.environ.get('NEWS_REFRESH_INTERVAL', 10)),   # minutes
        'initial_data_refresh_threshold': int(os.environ.get('DATA_THRESHOLD', 180)), # minutes
        'initial_news_refresh_threshold': int(os.environ.get('NEWS_THRESHOLD', 10))   # minutes
    }
    
    # File Paths
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
    LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
    PYTHON_SCRIPTS_DIR = os.path.join(SCRIPT_DIR, 'python')
    
    # News Script Configuration
    NEWS_SCRIPTS_WHITELIST = [
        'business_line.py', 'business_std.py', 'cnbctv_18.py',
        'econ_times.py', 'fin_exp.py', 'ft.py',
        'investing.py', 'money_control.py', 'ndtvprofit.py'
    ]
    
    # Data Scripts Configuration
    DATA_SCRIPTS = [
        'python/mutual_funds.py',
        'python/corp_actions.py',
        'python/volume_reports.py'
    ]

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    
class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    # Override sensitive settings from environment variables
    DB_CONFIG = {
        "dbname": os.environ.get('DB_NAME'),
        "user": os.environ.get('DB_USER'),
        "password": os.environ.get('DB_PASSWORD'),
        "host": os.environ.get('DB_HOST'),
        "port": os.environ.get('DB_PORT')
    }

class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    DEBUG = True

# Configuration mapping
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}