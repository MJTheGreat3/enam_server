"""
Routes package for Flask application.

This package contains all route handlers organized by functionality:
- main_routes: Page routes and template rendering
- api_routes: RESTful API endpoints
"""

from .main_routes import main_bp
from .api_routes import api_bp

__all__ = ['main_bp', 'api_bp']
