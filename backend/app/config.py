from dotenv import load_dotenv
import os

def load_config(app):
    load_dotenv()

    app.config['DB_NAME'] = os.getenv("DB_NAME")
    app.config['DB_USER'] = os.getenv("DB_USER")
    app.config['DB_PASSWORD'] = os.getenv("DB_PASSWORD")
    app.config['DB_HOST'] = os.getenv("DB_HOST")
    app.config['DB_PORT'] = os.getenv("DB_PORT")
    app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")
