from flask import Blueprint, render_template, request, jsonify
import threading
from psycopg2.extras import RealDictCursor
from backend.utils import get_db_connection, get_last_updated, set_last_updated
from backend.tasks import (
    run_all_data_scripts, run_all_news_scripts, run_company_scrapers_async,
    TEMP_LIST
)
from python.scrapers import company_data

routes = Blueprint("routes", __name__)

@routes.route('/')
def about():
    return render_template('about.html')

@routes.route('/block-deals')
def block_deals():
    return render_template('block.html')

@routes.route('/bulk-deals')
def bulk_deals():
    return render_template('bulk.html')

@routes.route('/news')
def news():
    return render_template('news.html')

@routes.route('/portfolio')
def portfolio():
    return render_template('portfolio.html')

@routes.route('/insider-deals')
def insider():
    return render_template('insider.html')

@routes.route('/corp-announcements')
def announcements():
    return render_template('announcements.html')

@routes.route('/corp-actions')
def actions():
    return render_template('actions.html')

@routes.route('/mutual-funds')
def mutual_funds():
    return render_template('mf.html')

@routes.route('/volume-reports')
def volume_reports():
    return render_template('volume.html')

# === Portfolio APIs ===
@routes.route("/api/portfolio", methods=["GET"])
def get_portfolio():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT Symbol AS symbol, Name AS name
            FROM symbols
            WHERE Status = TRUE;
        """)
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@routes.route("/api/portfolio", methods=["POST"])
def add_portfolio():
    data = request.get_json()
    symbol = data.get("symbol")
    if not symbol:
        return jsonify({"error": "Symbol required"}), 400

    symbol_upper = symbol.upper()

    conn = get_db_connection()
    with conn, conn.cursor() as cur:
        # Find the first record with this symbol
        cur.execute("""
            SELECT Tag_ID FROM symbols 
            WHERE Symbol = %s 
            LIMIT 1;
        """, (symbol_upper,))
        result = cur.fetchone()
        
        if not result:
            return jsonify({"error": f"No record found for symbol '{symbol_upper}'"}), 404
            
        tag_id = result[0]
        
        # Update the status of the found record
        cur.execute("""
            UPDATE symbols 
            SET Status = TRUE 
            WHERE Tag_ID = %s;
        """, (tag_id,))
        
    conn.close()

    TEMP_LIST.add(symbol_upper)
    return jsonify({"message": f"Symbol '{symbol_upper}' activated in portfolio"}), 200

@routes.route("/api/portfolio", methods=["DELETE"])
def remove_portfolio():
    data = request.get_json()
    symbol = data.get("symbol")
    if not symbol:
        return jsonify({"error": "Symbol required"}), 400

    symbol_upper = symbol.upper()

    conn = get_db_connection()
    with conn, conn.cursor() as cur:
        # Find the first record with this symbol
        cur.execute("""
            SELECT Tag_ID FROM symbols 
            WHERE Symbol = %s 
            LIMIT 1;
        """, (symbol_upper,))
        result = cur.fetchone()
        
        if not result:
            return jsonify({"error": f"No record found for symbol '{symbol_upper}'"}), 404
            
        tag_id = result[0]
        
        # Update the status of the found record
        cur.execute("""
            UPDATE symbols 
            SET Status = FALSE 
            WHERE Tag_ID = %s;
        """, (tag_id,))
        
    conn.close()

    TEMP_LIST.discard(symbol_upper)
    return jsonify({"message": f"Symbol '{symbol_upper}' deactivated from portfolio"}), 200

@routes.route("/api/portfolio/apply", methods=["POST"])
def apply_portfolio_changes():
    if not TEMP_LIST:
        return jsonify({"message": "No pending symbols to scrape."}), 200

    try:
        # Get symbols needing update
        symbols_to_scrape = list(TEMP_LIST)
        
        # Run scraping in background
        def scrape_task():
            try:
                # Scrape each symbol individually for better error handling
                for symbol in symbols_to_scrape:
                    try:
                        company_data.scrape_company_data(symbol)
                        TEMP_LIST.remove(symbol)
                    except Exception as e:
                        print(f"Failed to scrape {symbol}: {str(e)}")
                
                print("Company data scraping completed")
            except Exception as e:
                print(f"Scraping task failed: {str(e)}")

        # Start in background thread
        threading.Thread(target=scrape_task, daemon=True).start()
        
        return jsonify({
            "message": "Company data scraping initiated",
            "symbols": symbols_to_scrape
        }), 202  # Accepted status code
        
    except Exception as e:
        return jsonify({
            "error": "Failed to initiate scraping",
            "message": str(e)
        }), 500

# Add new endpoint for scraper status
@routes.route("/api/scrapers/status", methods=["GET"])
def get_scraper_status():
    conn = get_db_connection()
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
            
            return jsonify({
                "company_scraper": {
                    "total_symbols": stats[0],
                    "scraped_symbols": stats[1],
                    "last_scraped_time": stats[2].isoformat() if stats[2] else None,
                    "pending_symbols": list(TEMP_LIST)
                }
            })
    finally:
        conn.close()

@routes.route("/api/search-symbols", methods=["GET"])
def search_symbols():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT Symbol AS symbol, Name AS name
            FROM symbols
            WHERE Status = FALSE;
        """)
        results = cur.fetchall()
    conn.close()
    return jsonify(results)

@routes.route('/api/corp_actions')
def get_corp_actions():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT ca.*
            FROM corp_actions ca
            JOIN symbols s ON TRIM(ca.security_name) = TRIM(s.symbol)
            WHERE s.status = TRUE;
        """)
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@routes.route('/api/announcements')
def get_announcements():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT a.*
            FROM announcements a
            JOIN symbols s ON TRIM(a.stock) = TRIM(s.symbol)
            WHERE s.status = TRUE;
        """)
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@routes.route('/api/insider')
def get_insider_trading():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT i.*
            FROM insider_trading i
            JOIN symbols s ON TRIM(i.stock) = TRIM(s.symbol)
            WHERE s.status = TRUE;
        """)
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@routes.route('/api/block_deals')
def get_block_deals_data():
    conn = get_db_connection()
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
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@routes.route('/api/bulk_deals')
def get_bulk_deals_data():
    conn = get_db_connection()
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
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@routes.route('/api/volume')
def get_vol_deviation():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT v.*
            FROM vol_deviation v
            JOIN symbols s ON TRIM(v.symbol) = TRIM(s.symbol)
            WHERE s.status = TRUE;
        """)
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

@routes.route('/api/delivery')
def get_deliv_deviation():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT d.*
            FROM deliv_deviation d
            JOIN symbols s ON TRIM(d.symbol) = TRIM(s.symbol)
            WHERE s.status = TRUE;
        """)
        rows = cur.fetchall()
    conn.close()
    return jsonify(rows)

# === Refresh APIs ===
@routes.route('/api/refresh-data-sync', methods=['POST'])
def refresh_data_sync():
    logs = run_all_data_scripts()
    run_company_scrapers_async()
    return jsonify({
        "status": "success",
        "message": "Data refresh complete.",
        "logs": logs,
        "last_updated_data": get_last_updated("data")
    })

@routes.route('/api/refresh-news-sync', methods=['POST'])
def refresh_news_sync():
    logs = run_all_news_scripts()
    return jsonify({
        "status": "success",
        "message": "News refresh complete.",
        "logs": logs,
        "last_updated_news": get_last_updated("news")
    })

@routes.route('/api/last-updated-data', methods=['GET'])
def last_updated_data():
    return jsonify({"last_updated_data": get_last_updated("data")})

@routes.route('/api/last-updated-news', methods=['GET'])
def last_updated_news():
    return jsonify({"last_updated_news": get_last_updated("news")})