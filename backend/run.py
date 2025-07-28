from flask import Flask
from backend.routes import routes
from backend.scheduler import scheduler, schedule_jobs, run_scheduled_jobs

app = Flask(
    __name__,
    template_folder='../frontend/templates',
    static_folder='../frontend/static',
    static_url_path='/static'
)

app.register_blueprint(routes)

if __name__ == "__main__":
    scheduler.start()
    print("[INFO] Scheduler started.")
    schedule_jobs()
    run_scheduled_jobs()
    app.run(host='0.0.0.0', port=8000, debug=True)
