from flask import Blueprint, render_template
import pandas as pd
import glob
import os
from datetime import datetime
from config import Config

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def about():
    return render_template('about.html')

@main_bp.route('/block-deals')
def block_deals():
    return render_template('block.html')

@main_bp.route('/bulk-deals')
def bulk_deals():
    return render_template('bulk.html')

@main_bp.route('/news')
def news():
    return render_template('news.html')

@main_bp.route('/portfolio')
def portfolio():
    return render_template('portfolio.html')

@main_bp.route('/insider-deals')
def insider():
    return render_template('insider.html')

@main_bp.route('/corp-announcements')
def announcements():
    return render_template('announcements.html')

@main_bp.route('/corp-actions')
def actions():
    return render_template('actions.html')

@main_bp.route('/mutual-funds')
def mutual_funds():
    return render_template('mf.html')

@main_bp.route('/volume-reports')
def volume_reports():
    return render_template('volume.html')

@main_bp.route('/ath-matrix')
def ath_matrix():
    """ATH Matrix route with complex CSV processing logic"""
    # Use Config.SCRIPT_DIR (where config.py is) and look in the data folder
    csv_pattern = os.path.join(Config.SCRIPT_DIR, "data", "ATH_companies_with_market_cap_*.csv")
    
    files = sorted(glob.glob(csv_pattern))

    if not files:
        return render_template("ath_matrix.html",
                               table=[],
                               dates=[],
                               company_details={},
                               total_companies=0,
                               total_dates=0,
                               error_message=f"No ATH CSV files found in {os.path.join(Config.SCRIPT_DIR, 'data')}")

    presence = {}
    all_companies = set()
    all_dates = []
    company_details = {}

    for file in files:
        try:
            df = pd.read_csv(file)

            # Find company column
            company_col = None
            for col in ["Company", "Company Name", "company", "company_name"]:
                if col in df.columns:
                    company_col = col
                    break

            if company_col is None:
                continue

            # Find category column
            category_col = None
            for col in ["Category", "category", "Status"]:
                if col in df.columns:
                    category_col = col
                    break

            filename = os.path.basename(file)
            date_label = filename.replace("ATH_companies_with_market_cap_", "").replace(".csv", "")
            all_dates.append(date_label)

            for _, row in df.iterrows():
                company = str(row[company_col]).strip()
                if not company or company == 'nan':
                    continue

                all_companies.add(company)

                if company not in company_details:
                    sector = str(row.get('Sector', 'N/A')).strip()
                    industry = str(row.get('Industry', 'N/A')).strip()
                    market_cap = row.get('Market Cap (Cr)', 'N/A')

                    if sector == 'nan':
                        sector = 'N/A'
                    if industry == 'nan':
                        industry = 'N/A'

                    company_details[company] = {
                        'sector': sector,
                        'industry': industry,
                        'market_cap': market_cap,
                        'raw_market_cap': 0
                    }

                    if market_cap != 'N/A' and str(market_cap) != 'nan':
                        try:
                            market_cap_float = float(market_cap)
                            company_details[company]['raw_market_cap'] = market_cap_float
                            if market_cap_float >= 1000:
                                company_details[company]['market_cap_display'] = f"₹{market_cap_float/1000:.1f}K Cr"
                            else:
                                company_details[company]['market_cap_display'] = f"₹{market_cap_float:.1f} Cr"
                        except (ValueError, TypeError):
                            company_details[company]['market_cap_display'] = 'N/A'
                            company_details[company]['raw_market_cap'] = 0
                    else:
                        company_details[company]['market_cap_display'] = 'N/A'
                        company_details[company]['raw_market_cap'] = 0

                # Process category value
                category_value = 0
                if category_col and category_col in row.index:
                    try:
                        category_value = int(row[category_col])
                    except (ValueError, TypeError):
                        category_str = str(row[category_col]).strip().lower()
                        if 'new ath' in category_str or category_str == '0':
                            category_value = 0
                        elif 'within 5%' in category_str or category_str == '5':
                            category_value = 5
                        elif 'within 10%' in category_str or category_str == '10':
                            category_value = 10
                        else:
                            category_value = 0

                if company not in presence:
                    presence[company] = {}

                presence[company][date_label] = {
                    "present": True,
                    "category": category_value
                }

        except Exception as e:
            print(f"Error processing file {file}: {e}")  # Added error logging
            continue

    all_companies = sorted(list(all_companies))

    def parse_date(date_str):
        try:
            day, month, year = date_str.split('_')
            return datetime(int(year), int(month), int(day))
        except (ValueError, AttributeError):
            return datetime(1900, 1, 1)

    all_dates = sorted(all_dates, key=parse_date, reverse=True)

    table_data = []
    for company in all_companies:
        details = company_details.get(company, {})
        row = {
            'company': company,
            'sector': details.get('sector', 'N/A'),
            'industry': details.get('industry', 'N/A'),
            'market_cap': details.get('market_cap_display', 'N/A'),
            'raw_market_cap': details.get('raw_market_cap', 0),
            'presence': {}
        }

        for date in all_dates:
            company_presence = presence.get(company, {}).get(date, {"present": False, "category": None})

            if company_presence["present"]:
                category = company_presence["category"]
                if category == 0:
                    row['presence'][date] = {"status": "Yes", "class": "new-ath", "category": 0}
                elif category == 5:
                    row['presence'][date] = {"status": "Yes", "class": "within-5", "category": 5}
                elif category == 10:
                    row['presence'][date] = {"status": "No", "class": "within-10", "category": 10}
                else:
                    row['presence'][date] = {"status": "Yes", "class": "new-ath", "category": 0}
            else:
                row['presence'][date] = {"status": "No", "class": "not-present", "category": None}

        table_data.append(row)

    return render_template("ath_matrix.html",
                           table=table_data,
                           dates=all_dates,
                           company_details=company_details,
                           total_companies=len(all_companies),
                           total_dates=len(all_dates))