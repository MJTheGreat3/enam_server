from flask import Blueprint, render_template
from services.file_service import FileService

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def about():
    """About page - main landing page"""
    return render_template('about.html')

@main_bp.route('/block-deals')
def block_deals():
    """Block deals page"""
    return render_template('block.html')

@main_bp.route('/bulk-deals')
def bulk_deals():
    """Bulk deals page"""
    return render_template('bulk.html')

@main_bp.route('/news')
def news():
    """News page"""
    return render_template('news.html')

@main_bp.route('/portfolio')
def portfolio():
    """Portfolio management page"""
    return render_template('portfolio.html')

@main_bp.route('/insider-deals')
def insider():
    """Insider deals page"""
    return render_template('insider.html')

@main_bp.route('/corp-announcements')
def announcements():
    """Corporate announcements page"""
    return render_template('announcements.html')

@main_bp.route('/corp-actions')
def actions():
    """Corporate actions page"""
    return render_template('actions.html')

@main_bp.route('/mutual-funds')
def mutual_funds():
    """Mutual funds page"""
    return render_template('mf.html')

@main_bp.route('/volume-reports')
def volume_reports():
    """Volume reports page"""
    return render_template('volume.html')

@main_bp.route('/ath-matrix')
def ath_matrix():
    """All-time high matrix page"""
    try:
        file_service = FileService()
        table_data, dates, company_details, stats, error_message = file_service.process_ath_files()
        
        return render_template(
            "ath_matrix.html",
            table=table_data,
            dates=dates,
            company_details=company_details,
            total_companies=stats.get('total_companies', 0),
            total_dates=stats.get('total_dates', 0),
            error_message=error_message
        )
    except Exception as e:
        return render_template(
            "ath_matrix.html",
            table=[],
            dates=[],
            company_details={},
            total_companies=0,
            total_dates=0,
            error_message=f"Error processing ATH data: {str(e)}"
        )