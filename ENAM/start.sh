#!/bin/bash

# Flask Application Startup Script
# This script handles the startup of the Flask application with proper logging and error handling

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create necessary directories
print_status "Creating necessary directories..."
mkdir -p logs data frontend/static frontend/templates

# Set permissions
print_status "Setting up permissions..."
chmod +x start.sh 2>/dev/null || true

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    print_error "Python3 is not installed or not in PATH"
    exit 1
fi

# Check if required files exist
if [ ! -f "requirements.txt" ]; then
    print_error "requirements.txt not found"
    exit 1
fi

if [ ! -f "wsgi.py" ]; then
    print_error "wsgi.py not found"
    exit 1
fi

# Install dependencies if they don't exist
print_status "Checking Python dependencies..."
pip3 install -r requirements.txt --quiet || {
    print_error "Failed to install Python dependencies"
    exit 1
}

# Set environment variables
export FLASK_APP=wsgi.py
export FLASK_ENV=production
export PYTHONPATH=$(pwd)

print_status "Environment variables set:"
print_status "  FLASK_APP=$FLASK_APP"
print_status "  FLASK_ENV=$FLASK_ENV"
print_status "  PYTHONPATH=$PYTHONPATH"

# Function to handle cleanup on exit
cleanup() {
    print_warning "Received shutdown signal, cleaning up..."
    if [ ! -z "$GUNICORN_PID" ]; then
        kill $GUNICORN_PID 2>/dev/null || true
    fi
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Check if gunicorn is available
if command -v gunicorn &> /dev/null; then
    print_status "Starting application with Gunicorn..."
    
    # Start with gunicorn
    if [ -f "gunicorn.conf.py" ]; then
        gunicorn --config gunicorn.conf.py wsgi:app &
    else
        gunicorn --bind 0.0.0.0:8000 --workers 4 --timeout 120 wsgi:app &
    fi
    
    GUNICORN_PID=$!
    print_status "Gunicorn started with PID: $GUNICORN_PID"
    
    # Wait for gunicorn to finish
    wait $GUNICORN_PID
    
else
    print_warning "Gunicorn not found, falling back to Flask development server"
    print_warning "This is not recommended for production use!"
    
    # Fallback to Flask development server
    python3 wsgi.py
fi

print_status "Application has stopped."