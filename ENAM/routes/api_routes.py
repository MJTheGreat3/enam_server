from flask import Blueprint, request, jsonify
from services.portfolio_service import (
    get_portfolio_data, 
    add_portfolio_item, 
    remove_portfolio_item, 
    apply_portfolio_changes
)
from services.data_service import run_all_data_scripts, run_all_news_scripts
from services.file_service import get_last_updated
from config import Config

api_bp = Blueprint('api', __name__)

# === Portfolio APIs ===
@api_bp.route("/portfolio", methods=["GET"])
def get_portfolio():
    """Get all portfolio items"""
    return jsonify(get_portfolio_data())

@api_bp.route("/portfolio", methods=["POST"])
def add_portfolio():
    """Add a new portfolio item"""
    data = request.get_json()
    symbol = data.get("symbol")
    name = data.get("name")
    
    if not symbol or not name:
        return jsonify({"error": "Missing data"}), 400
    
    try:
        add_portfolio_item(symbol, name)
        return jsonify({"message": "Added"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/portfolio", methods=["DELETE"])
def remove_portfolio():
    """Remove a portfolio item"""
    data = request.get_json()
    symbol = data.get("symbol")
    
    if not symbol:
        return jsonify({"error": "Missing symbol"}), 400
    
    try:
        if remove_portfolio_item(symbol):
            return jsonify({"message": "Deleted"}), 200
        else:
            return jsonify({"error": "Not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/portfolio/apply", methods=["POST"])
def apply_portfolio_changes():
    """Apply portfolio changes by running scraper"""
    try:
        apply_portfolio_changes()
        return jsonify({"message": "Scraper run"}), 200
    except Exception as e:
        print(f"[ERROR] Apply route: {e}")
        return jsonify({"error": "Scraper failed"}), 500

# === Refresh APIs ===
@api_bp.route('/refresh-data-sync', methods=['POST'])
def refresh_data_sync():
    """Synchronously refresh data"""
    logs = run_all_data_scripts()
    return jsonify({
        "status": "success",
        "message": "Data refresh complete.",
        "logs": logs,
        "last_updated_data": get_last_updated(Config.LAST_UPDATED_DATA_FILE)
    })

@api_bp.route('/refresh-news-sync', methods=['POST'])
def refresh_news_sync():
    """Synchronously refresh news"""
    logs = run_all_news_scripts()
    return jsonify({
        "status": "success",
        "message": "News refresh complete.",
        "logs": logs,
        "last_updated_news": get_last_updated(Config.LAST_UPDATED_NEWS_FILE)
    })

@api_bp.route('/last-updated-data', methods=['GET'])
def last_updated_data():
    """Get last data update timestamp"""
    return jsonify({
        "last_updated_data": get_last_updated(Config.LAST_UPDATED_DATA_FILE)
    })

@api_bp.route('/last-updated-news', methods=['GET'])
def last_updated_news():
    """Get last news update timestamp"""
    return jsonify({
        "last_updated_news": get_last_updated(Config.LAST_UPDATED_NEWS_FILE)
    })