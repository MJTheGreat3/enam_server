#!/bin/bash

# Exit on any error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Enam App...${NC}"

# Create necessary directories
echo -e "${YELLOW}Creating necessary directories...${NC}"
mkdir -p data
mkdir -p logs

# Set environment variables if not already set
export FLASK_CONFIG=${FLASK_CONFIG:-production}
export PORT=${PORT:-8000}

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv venv
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source venv/bin/activate

# Install/upgrade dependencies
echo -e "${YELLOW}Installing dependencies...${NC}"
pip install --upgrade pip
pip install -r requirements.txt

# Check database connection
echo -e "${YELLOW}Checking database connection...${NC}"
python -c "
import psycopg2
from config import config
import os

config_name = os.environ.get('FLASK_CONFIG', 'production')
db_config = config[config_name].DB_CONFIG

try:
    conn = psycopg2.connect(**db_config)
    conn.close()
    print('Database connection successful')
except Exception as e:
    print(f'Database connection failed: {e}')
    exit(1)
"

# Start the application
echo -e "${GREEN}Starting application with Gunicorn...${NC}"

if [ "$FLASK_CONFIG" = "development" ]; then
    echo -e "${YELLOW}Running in development mode...${NC}"
    python app.py
else
    echo -e "${YELLOW}Running in production mode with Gunicorn...${NC}"
    gunicorn --config gunicorn.conf.py wsgi:application
fi