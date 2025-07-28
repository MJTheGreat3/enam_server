from flask import Flask
from app.config import load_config
from app.scheduler import scheduler
from app.routes import register_routes

def create_app():
    app = Flask(__name__, static_url_path='/static')
    load_config(app)
    
    scheduler.init_app(app)
    scheduler.start()
    
    register_routes(app)
    return app
