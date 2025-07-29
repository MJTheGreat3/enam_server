import csv
import os
from config import Config
from python import scraper

def get_portfolio_data():
    """Get all portfolio items from CSV file"""
    if not os.path.exists(Config.PORTFOLIO_FILE):
        return []
    
    portfolio = []
    with open(Config.PORTFOLIO_FILE, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            portfolio.append({
                "symbol": row["symbol"],
                "name": row["name"],
                "status": row.get("status", "Old")
            })
    return portfolio

def add_portfolio_item(symbol, name):
    """Add or update a portfolio item"""
    rows = []
    found = False

    if os.path.exists(Config.PORTFOLIO_FILE):
        with open(Config.PORTFOLIO_FILE, "r", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                header = ["symbol", "name", "status"]
            
            for row in reader:
                if len(row) >= 1 and row[0].upper() == symbol.upper():
                    # Update existing item
                    row = [row[0], name if len(row) < 2 else row[1], "New"]
                    found = True
                rows.append(row)
    else:
        header = ["symbol", "name", "status"]

    if not found:
        rows.append([symbol.upper(), name, "New"])

    # Ensure directory exists
    dir_path = os.path.dirname(Config.PORTFOLIO_FILE)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    
    # Write updated data
    with open(Config.PORTFOLIO_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

def remove_portfolio_item(symbol):
    """Remove a portfolio item"""
    if not os.path.exists(Config.PORTFOLIO_FILE):
        return False
    
    rows = []
    found = False
    
    with open(Config.PORTFOLIO_FILE, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return False
            
        for row in reader:
            if len(row) >= 1 and row[0].upper() == symbol.upper():
                found = True
                continue  # Skip this row (delete it)
            rows.append(row)

    if found:
        with open(Config.PORTFOLIO_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
    
    return found

def apply_portfolio_changes():
    """Apply portfolio changes by running the scraper"""
    scraper.main("new")