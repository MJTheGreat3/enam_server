import os
import subprocess
import threading
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from services.data_service import DataService

class SchedulerService(DataService):
    """Service for managing background scheduled jobs"""
    
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.scheduler = BackgroundScheduler()
        self.scheduler_enabled = {"data": True, "news": True}
        self.script_lock = threading.Lock()
        
        # Create locks for news scripts
        self.news_script_locks = {
            script: threading.Lock() 
            for script in config['NEWS_SCRIPTS_WHITELIST']
        }
        
        # Job IDs
        self.DATA_JOB_ID = "data_refresh_job"
        self.NEWS_JOB_ID = "news_refresh_job"
    
    def start(self):
        """Start the scheduler"""
        if not self.scheduler.running:
            self.scheduler.start()
            self._schedule_jobs()
    
    def stop(self):
        """Stop the scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
    
    def _schedule_jobs(self):
        """Schedule jobs based on current state"""
        if self.scheduler_enabled["data"]:
            self.scheduler.add_job(
                self.run_all_data_scripts,
                'interval',
                minutes=self.config['SCHEDULER_CONFIG']['data_refresh_interval'],
                id=self.DATA_JOB_ID,
                replace_existing=True
            )
        
        if self.scheduler_enabled["news"]:
            self.scheduler.add_job(
                self.run_all_news_scripts,
                'interval',
                minutes=self.config['SCHEDULER_CONFIG']['news_refresh_interval'],
                id=self.NEWS_JOB_ID,
                replace_existing=True
            )
    
    def run_initial_jobs(self):
        """Run initial jobs if needed based on last update time"""
        print("[INFO] Checking whether initial data and news updates are needed...")
        now = datetime.datetime.now()
        
        # Check if data needs update
        last_data_str = self.get_last_updated("data")
        data_needs_update = not last_data_str or \
            (now - datetime.datetime.strptime(last_data_str, "%Y-%m-%d %H:%M:%S")).total_seconds() > \
            self.config['SCHEDULER_CONFIG']['initial_data_refresh_threshold'] * 60
        
        # Check if news needs update
        last_news_str = self.get_last_updated("news")
        news_needs_update = not last_news_str or \
            (now - datetime.datetime.strptime(last_news_str, "%Y-%m-%d %H:%M:%S")).total_seconds() > \
            self.config['SCHEDULER_CONFIG']['initial_news_refresh_threshold'] * 60
        
        def background_job():
            logs = []
            if data_needs_update:
                logs.extend(self.run_all_data_scripts())
            if news_needs_update:
                logs.extend(self.run_all_news_scripts())
            print("[INITIAL SCHEDULED RUN LOGS]")
            print("\n".join(logs))
        
        if data_needs_update or news_needs_update:
            threading.Thread(target=background_job, daemon=True).start()
        else:
            print("[INFO] No initial refresh needed.")
    
    def run_cleaner(self):
        """Run the cleaner script"""
        logs = []
        cleaner_path = os.path.join(self.config['PYTHON_SCRIPTS_DIR'], 'cleaner.py')
        
        if os.path.exists(cleaner_path):
            logs.append("[INFO] Running cleaner.py...")
            try:
                result = subprocess.run(
                    ['python', 'cleaner.py'],
                    cwd=self.config['PYTHON_SCRIPTS_DIR'],
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
    
    def run_python_script(self, script_path):
        """Run a Python script with proper logging"""
        logs = []
        script_name = os.path.basename(script_path)
        
        if not os.path.exists(script_path):
            logs.append(f"[ERROR] Script not found: {script_path}")
            return logs
        
        # Use appropriate lock
        lock = self.news_script_locks.get(script_name, self.script_lock)
        
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
    
    def run_all_data_scripts(self):
        """Run all data collection scripts"""
        logs = []
        
        # Run bulk/block scrapers
        logs.append("[INFO] Running bulk/block scrapers...")
        try:
            result = subprocess.run(
                ['python', 'scraper.py', 'all'],
                cwd=self.config['PYTHON_SCRIPTS_DIR'],
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
        
        # Run other data scripts
        for script in filter(os.path.exists, self.config['DATA_SCRIPTS']):
            logs.extend(self.run_python_script(script))
        
        # Run company scrapers asynchronously
        self._run_company_scrapers_async()
        
        # Update timestamp
        self.set_last_updated("data")
        return logs
    
    def run_all_news_scripts(self):
        """Run all news collection scripts"""
        logs = []
        news_folder = os.path.join(self.config['PYTHON_SCRIPTS_DIR'], 'news')
        
        scripts = [
            os.path.join(news_folder, s) for s in self.config['NEWS_SCRIPTS_WHITELIST']
            if os.path.exists(os.path.join(news_folder, s))
        ]
        
        if not scripts:
            logs.append("[WARNING] No news scripts found to run.")
            return logs
        
        logs.append(f"[INFO] Found {len(scripts)} news scripts to run.")
        
        for script in scripts:
            logs.append(f"[INFO] Running news script: {script}")
            logs.extend(self.run_python_script(script))
            
            logs.append("[INFO] Running cleaner after news script.")
            logs.extend(self.run_cleaner())
        
        # Update timestamp
        self.set_last_updated("news")
        logs.append("[INFO] All news scripts (and cleaning) complete.")
        return logs
    
    def _run_company_scrapers_async(self):
        """Run company data scrapers asynchronously"""
        def target():
            try:
                logs = []
                logs.append("[INFO] Running company data scrapers...")
                # Import here to avoid circular imports
                from python.scrapers.company_data import run_company_scrapers
                run_company_scrapers()
                logs.append("[SUCCESS] Company data scraping completed")
                print("\n".join(logs))
            except Exception as e:
                print(f"[ERROR] Company scraping failed: {str(e)}")
        
        threading.Thread(target=target, daemon=True).start()
    
    def refresh_data_sync(self):
        """Synchronously refresh data"""
        logs = self.run_all_data_scripts()
        return {
            "status": "success",
            "message": "Data refresh complete.",
            "logs": logs,
            "last_updated_data": self.get_last_updated("data")
        }
    
    def refresh_news_sync(self):
        """Synchronously refresh news"""
        logs = self.run_all_news_scripts()
        return {
            "status": "success",
            "message": "News refresh complete.",
            "logs": logs,
            "last_updated_news": self.get_last_updated("news")
        }
    
    def get_status(self):
        """Get scheduler status"""
        return self.scheduler_enabled.copy()
    
    def toggle_job(self, key, enable):
        """Toggle scheduler job for data or news"""
        self.scheduler_enabled[key] = bool(enable)
        job_id = self.DATA_JOB_ID if key == "data" else self.NEWS_JOB_ID
        
        # Remove existing job if present
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
        
        # Add job if enabling
        if enable:
            job_func = self.run_all_data_scripts if key == "data" else self.run_all_news_scripts
            interval = (self.config['SCHEDULER_CONFIG']['data_refresh_interval'] 
                       if key == "data" 
                       else self.config['SCHEDULER_CONFIG']['news_refresh_interval'])
            
            self.scheduler.add_job(job_func, 'interval', minutes=interval, id=job_id)
        
        return {"message": f"{key.capitalize()} scheduler {'enabled' if enable else 'disabled'}."}