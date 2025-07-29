from app import create_app, setup_scheduler
from services.scheduler_service import run_scheduled_jobs

# Create the Flask application
app = create_app()

# Setup scheduler for production
scheduler = setup_scheduler()

# Run initial scheduled jobs
run_scheduled_jobs()

if __name__ == "__main__":
    app.run()