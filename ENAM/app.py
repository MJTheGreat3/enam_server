import os
from flask import Flask
from config import config
from services.scheduler_service import SchedulerService
from routes.main_routes import main_bp
from routes.api_routes import api_bp

def create_app(config_name=None):
    """Flask application factory"""
    
    if config_name is None:
        config_name = os.environ.get('FLASK_CONFIG', 'default')
    
    app = Flask(
        __name__,
        template_folder='frontend/templates',
        static_folder='frontend/static',
        static_url_path='/static'
    )
    
    # Load configuration
    app.config.from_object(config[config_name])
    
    # Ensure required directories exist
    os.makedirs(app.config['DATA_DIR'], exist_ok=True)
    os.makedirs(app.config['LOGS_DIR'], exist_ok=True)
    
    # Register blueprints
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    
    # Initialize scheduler service
    scheduler_service = SchedulerService(app.config)
    
    # Store scheduler service in app context for access in routes
    app.scheduler_service = scheduler_service
    
    # Initialize scheduler immediately after app creation
    try:
        scheduler_service.start()
        scheduler_service.run_initial_jobs()
        app.logger.info("Scheduler initialized successfully")
    except Exception as e:
        app.logger.error(f"Failed to initialize scheduler: {str(e)}")
    
    @app.teardown_appcontext
    def close_scheduler(exception):
        """Clean up scheduler on app teardown"""
        if hasattr(app, 'scheduler_service'):
            try:
                app.scheduler_service.stop()
            except Exception as e:
                app.logger.error(f"Error stopping scheduler: {str(e)}")
    
    return app

# For development server
if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=8000, debug=True)