from flask import Blueprint, request, jsonify, current_app
from services.portfolio_service import PortfolioService
from services.data_service import DataService
from services.scheduler_service import SchedulerService

api_bp = Blueprint('api', __name__)

# Initialize services
portfolio_service = PortfolioService()
data_service = DataService()

# === Portfolio Management APIs ===

@api_bp.route("/portfolio", methods=["GET"])
def get_portfolio():
    """Get all active portfolio symbols"""
    try:
        symbols = portfolio_service.get_active_symbols()
        return jsonify(symbols)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/portfolio", methods=["POST"])
def add_portfolio():
    """Add symbol to portfolio"""
    try:
        data = request.get_json()
        symbol = data.get("symbol")
        
        if not symbol:
            return jsonify({"error": "Symbol required"}), 400
        
        result = portfolio_service.add_symbol(symbol)
        return jsonify(result), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/portfolio", methods=["DELETE"])
def remove_portfolio():
    """Remove symbol from portfolio"""
    try:
        data = request.get_json()
        symbol = data.get("symbol")
        
        if not symbol:
            return jsonify({"error": "Symbol required"}), 400
        
        result = portfolio_service.remove_symbol(symbol)
        return jsonify(result), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/portfolio/apply", methods=["POST"])
def apply_portfolio_changes():
    """Apply pending portfolio changes by scraping company data"""
    try:
        result = portfolio_service.apply_changes()
        return jsonify(result), 202  # Accepted status
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/search-symbols", methods=["GET"])
def search_symbols():
    """Search for available symbols not in portfolio"""
    try:
        symbols = portfolio_service.get_available_symbols()
        return jsonify(symbols)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/scrapers/status", methods=["GET"])
def get_scraper_status():
    """Get scraper status information"""
    try:
        status = portfolio_service.get_scraper_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# === Data Retrieval APIs ===

@api_bp.route('/corp_actions')
def get_corp_actions():
    """Get corporate actions for portfolio symbols"""
    try:
        data = data_service.get_corp_actions()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/announcements')
def get_announcements():
    """Get announcements for portfolio symbols"""
    try:
        data = data_service.get_announcements()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/insider')
def get_insider_trading():
    """Get insider trading data for portfolio symbols"""
    try:
        data = data_service.get_insider_trading()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/block_deals')
def get_block_deals_data():
    """Get block deals data for portfolio symbols"""
    try:
        data = data_service.get_block_deals()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/bulk_deals')
def get_bulk_deals_data():
    """Get bulk deals data for portfolio symbols"""
    try:
        data = data_service.get_bulk_deals()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/volume')
def get_vol_deviation():
    """Get volume deviation data for portfolio symbols"""
    try:
        data = data_service.get_volume_deviation()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/delivery')
def get_deliv_deviation():
    """Get delivery deviation data for portfolio symbols"""
    try:
        data = data_service.get_delivery_deviation()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/news')
def get_news():
    """Get latest news data"""
    try:
        data = data_service.get_news()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# === Data Refresh APIs ===

@api_bp.route('/refresh-data-sync', methods=['POST'])
def refresh_data_sync():
    """Trigger synchronous data refresh"""
    try:
        scheduler_service = current_app.scheduler_service
        result = scheduler_service.refresh_data_sync()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/refresh-news-sync', methods=['POST'])
def refresh_news_sync():
    """Trigger synchronous news refresh"""
    try:
        scheduler_service = current_app.scheduler_service
        result = scheduler_service.refresh_news_sync()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/last-updated-data', methods=['GET'])
def last_updated_data():
    """Get last data update timestamp"""
    try:
        timestamp = data_service.get_last_updated("data")
        return jsonify({"last_updated_data": timestamp})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/last-updated-news', methods=['GET'])
def last_updated_news():
    """Get last news update timestamp"""
    try:
        timestamp = data_service.get_last_updated("news")
        return jsonify({"last_updated_news": timestamp})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# === Scheduler Management APIs ===

@api_bp.route("/scheduler", methods=["GET"])
def get_scheduler_status():
    """Get scheduler status"""
    try:
        scheduler_service = current_app.scheduler_service
        status = scheduler_service.get_status()
        return jsonify(status)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/scheduler", methods=["POST"])
def toggle_scheduler():
    """Toggle scheduler for data or news"""
    try:
        data = request.get_json()
        key = data.get("key")
        enable = data.get("enable")
        
        if key not in ["data", "news"]:
            return jsonify({"error": "Invalid scheduler key"}), 400
        
        scheduler_service = current_app.scheduler_service
        result = scheduler_service.toggle_job(key, enable)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500