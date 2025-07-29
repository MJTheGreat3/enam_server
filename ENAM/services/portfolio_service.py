import threading
from psycopg2.extras import RealDictCursor
from services.data_service import DataService

class PortfolioService(DataService):
    """Service for managing portfolio operations"""
    
    def __init__(self):
        super().__init__()
        self.temp_list = set()  # Symbols pending scraping
    
    def get_active_symbols(self):
        """Get all active portfolio symbols"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT Symbol AS symbol, Name AS name
                    FROM symbols
                    WHERE Status = TRUE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_available_symbols(self):
        """Get all available symbols not in portfolio"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT Symbol AS symbol, Name AS name
                    FROM symbols
                    WHERE Status = FALSE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def add_symbol(self, symbol):
        """Add symbol to portfolio"""
        symbol_upper = symbol.upper()
        
        conn = self.get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                # Find the first record with this symbol
                cur.execute("""
                    SELECT Tag_ID FROM symbols 
                    WHERE Symbol = %s 
                    LIMIT 1;
                """, (symbol_upper,))
                result = cur.fetchone()
                
                if not result:
                    raise ValueError(f"No record found for symbol '{symbol_upper}'")
                    
                tag_id = result[0]
                
                # Update the status of the found record
                cur.execute("""
                    UPDATE symbols 
                    SET Status = TRUE 
                    WHERE Tag_ID = %s;
                """, (tag_id,))
                
                self.temp_list.add(symbol_upper)
                return {"message": f"Symbol '{symbol_upper}' activated in portfolio"}
                
        finally:
            conn.close()
    
    def remove_symbol(self, symbol):
        """Remove symbol from portfolio"""
        symbol_upper = symbol.upper()
        
        conn = self.get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                # Find the first record with this symbol
                cur.execute("""
                    SELECT Tag_ID FROM symbols 
                    WHERE Symbol = %s 
                    LIMIT 1;
                """, (symbol_upper,))
                result = cur.fetchone()
                
                if not result:
                    raise ValueError(f"No record found for symbol '{symbol_upper}'")
                    
                tag_id = result[0]
                
                # Update the status of the found record
                cur.execute("""
                    UPDATE symbols 
                    SET Status = FALSE 
                    WHERE Tag_ID = %s;
                """, (tag_id,))
                
                self.temp_list.discard(symbol_upper)
                return {"message": f"Symbol '{symbol_upper}' deactivated from portfolio"}
                
        finally:
            conn.close()
    
    def apply_changes(self):
        """Apply portfolio changes by scraping company data"""
        if not self.temp_list:
            return {"message": "No pending symbols to scrape."}
        
        symbols_to_scrape = list(self.temp_list)
        
        def scrape_task():
            """Background task to scrape company data"""
            try:
                # Import here to avoid circular imports
                from python.scrapers.company_data import scrape_company_data
                
                for symbol in symbols_to_scrape:
                    try:
                        scrape_company_data(symbol)
                        self.temp_list.discard(symbol)
                    except Exception as e:
                        print(f"Failed to scrape {symbol}: {str(e)}")
                        
                print("Company data scraping completed")
            except Exception as e:
                print(f"Scraping task failed: {str(e)}")
        
        # Start background thread
        threading.Thread(target=scrape_task, daemon=True).start()
        
        return {
            "message": "Company data scraping initiated",
            "symbols": symbols_to_scrape
        }
    
    def get_scraper_status(self):
        """Get current scraper status"""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                # Check company scraper status
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_symbols,
                        COUNT(CASE WHEN last_scraped IS NOT NULL THEN 1 END) as scraped_symbols,
                        MAX(last_scraped) as last_scraped_time
                    FROM symbols 
                    WHERE status = TRUE
                """)
                stats = cur.fetchone()
                
                return {
                    "company_scraper": {
                        "total_symbols": stats[0],
                        "scraped_symbols": stats[1],
                        "last_scraped_time": stats[2].isoformat() if stats[2] else None,
                        "pending_symbols": list(self.temp_list)
                    }
                }
        finally:
            conn.close()