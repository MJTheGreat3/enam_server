import os
import glob
import pandas as pd
from datetime import datetime
from config import Config

class FileService:
    """Service for handling file operations"""
    
    def __init__(self):
        self.script_dir = Config.SCRIPT_DIR
    
    def process_ath_files(self):
        """Process ATH (All Time High) CSV files and return processed data"""
        csv_pattern = os.path.join(Config.DATA_DIR, "ATH_companies_with_market_cap_*.csv")
        files = sorted(glob.glob(csv_pattern))
        
        if not files:
            return [], [], {}, {"total_companies": 0, "total_dates": 0}, "No ATH CSV files found"
        
        presence = {}
        all_companies = set()
        all_dates = []
        company_details = {}
        
        for file in files:
            try:
                df = pd.read_csv(file)
                
                # Find company column
                company_col = self._find_column(df, ["Company", "Company Name", "company", "company_name"])
                if company_col is None:
                    continue
                
                # Find category column
                category_col = self._find_column(df, ["Category", "category", "Status"])
                
                # Extract date from filename
                filename = os.path.basename(file)
                date_label = filename.replace("ATH_companies_with_market_cap_", "").replace(".csv", "")
                all_dates.append(date_label)
                
                # Process each row
                for _, row in df.iterrows():
                    company = str(row[company_col]).strip()
                    if not company or company == 'nan':
                        continue
                    
                    all_companies.add(company)
                    
                    # Store company details if not already stored
                    if company not in company_details:
                        company_details[company] = self._extract_company_details(row)
                    
                    # Process category
                    category_value = self._process_category(row, category_col)
                    
                    # Store presence data
                    if company not in presence:
                        presence[company] = {}
                    
                    presence[company][date_label] = {
                        "present": True,
                        "category": category_value
                    }
                    
            except Exception as e:
                continue
        
        # Sort data
        all_companies = sorted(list(all_companies))
        all_dates = sorted(all_dates, key=self._parse_date, reverse=True)
        
        # Build table data
        table_data = self._build_table_data(all_companies, all_dates, presence, company_details)
        
        stats = {
            "total_companies": len(all_companies),
            "total_dates": len(all_dates)
        }
        
        return table_data, all_dates, company_details, stats, None
    
    def _find_column(self, df, column_names):
        """Find the first matching column name in DataFrame"""
        for col in column_names:
            if col in df.columns:
                return col
        return None
    
    def _extract_company_details(self, row):
        """Extract company details from a row"""
        sector = str(row.get('Sector', 'N/A')).strip()
        industry = str(row.get('Industry', 'N/A')).strip()
        market_cap = row.get('Market Cap (Cr)', 'N/A')
        
        if sector == 'nan':
            sector = 'N/A'
        if industry == 'nan':
            industry = 'N/A'
        
        details = {
            'sector': sector,
            'industry': industry,
            'market_cap': market_cap,
            'raw_market_cap': 0
        }
        
        # Process market cap display
        if market_cap != 'N/A' and str(market_cap) != 'nan':
            try:
                market_cap_float = float(market_cap)
                details['raw_market_cap'] = market_cap_float
                if market_cap_float >= 1000:
                    details['market_cap_display'] = f"₹{market_cap_float/1000:.1f}K Cr"
                else:
                    details['market_cap_display'] = f"₹{market_cap_float:.1f} Cr"
            except (ValueError, TypeError):
                details['market_cap_display'] = 'N/A'
                details['raw_market_cap'] = 0
        else:
            details['market_cap_display'] = 'N/A'
            details['raw_market_cap'] = 0
        
        return details
    
    def _process_category(self, row, category_col):
        """Process category value from row"""
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
        
        return category_value
    
    def _parse_date(self, date_str):
        """Parse date string for sorting"""
        try:
            day, month, year = date_str.split('_')
            return datetime(int(year), int(month), int(day))
        except (ValueError, AttributeError):
            return datetime(1900, 1, 1)
    
    def _build_table_data(self, all_companies, all_dates, presence, company_details):
        """Build table data structure for template"""
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
        
        return table_data