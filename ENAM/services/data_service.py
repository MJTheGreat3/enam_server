import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
from config import Config

class DataService:
    """Service for handling database operations and data retrieval"""
    
    def __init__(self):
        self.db_config = Config.DB_CONFIG
    
    def get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def set_last_updated(self, key):
        """Set last updated timestamp for a given key"""
        conn = self.get_db_connection()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO last_updated (key, timestamp)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET timestamp = EXCLUDED.timestamp;
                """, (key, datetime.datetime.now()))
        finally:
            conn.close()
    
    def get_last_updated(self, key):
        """Get last updated timestamp for a given key"""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT timestamp FROM last_updated WHERE key = %s;", (key,))
                result = cur.fetchone()
            return result[0].strftime("%Y-%m-%d %H:%M:%S") if result else None
        finally:
            conn.close()
    
    def get_corp_actions(self):
        """Get corporate actions for active portfolio symbols"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT ca.*
                    FROM corp_actions ca
                    JOIN symbols s ON TRIM(ca.security_name) = TRIM(s.symbol)
                    WHERE s.status = TRUE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_announcements(self):
        """Get announcements for active portfolio symbols"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT a.*
                    FROM announcements a
                    JOIN symbols s ON TRIM(a.stock) = TRIM(s.symbol)
                    WHERE s.status = TRUE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_insider_trading(self):
        """Get insider trading data for active portfolio symbols"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT i.*
                    FROM insider_trading i
                    JOIN symbols s ON TRIM(i.stock) = TRIM(s.symbol)
                    WHERE s.status = TRUE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_block_deals(self):
        """Get block deals data for active portfolio symbols"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        b.source,
                        b.deal_date,
                        b.security_name,
                        b.client_name,
                        b.deal_type,
                        b.quantity,
                        b.trade_price
                    FROM block_deals b
                    JOIN symbols s ON TRIM(b.security_name) = TRIM(s.symbol)
                    WHERE s.status = TRUE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_bulk_deals(self):
        """Get bulk deals data for active portfolio symbols"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        b.source,
                        b.deal_date,
                        b.security_name,
                        b.client_name,
                        b.deal_type,
                        b.quantity,
                        b.price
                    FROM bulk_deals b
                    JOIN symbols s ON TRIM(b.security_name) = TRIM(s.symbol)
                    WHERE s.status = TRUE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_volume_deviation(self):
        """Get volume deviation data for active portfolio symbols"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT v.*
                    FROM vol_deviation v
                    JOIN symbols s ON TRIM(v.symbol) = TRIM(s.symbol)
                    WHERE s.status = TRUE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_delivery_deviation(self):
        """Get delivery deviation data for active portfolio symbols"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT d.*
                    FROM deliv_deviation d
                    JOIN symbols s ON TRIM(d.symbol) = TRIM(s.symbol)
                    WHERE s.status = TRUE;
                """)
                return cur.fetchall()
        finally:
            conn.close()
    
    def get_news(self):
        """Get latest news data"""
        conn = self.get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT source, headline, link, category, time 
                    FROM news
                    WHERE headline IS NOT NULL AND time IS NOT NULL
                    ORDER BY time DESC
                """)
                return cur.fetchall()
        finally:
            conn.close()