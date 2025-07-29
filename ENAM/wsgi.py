import os
from app import create_app

# Create the Flask application instance
application = create_app(os.environ.get('FLASK_CONFIG', 'production'))

if __name__ == "__main__":
    application.run()