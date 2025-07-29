function filterByMarketCap() {
    const filterValue = document.getElementById('marketCapFilter').value;
    const rows = document.querySelectorAll('.ath-matrix-table tbody tr');
    let displayedCount = 0;
    
    rows.forEach(row => {
        const marketCap = parseFloat(row.getAttribute('data-market-cap')) || 0;
        let shouldShow = true;
        
        if (filterValue !== 'all') {
            const threshold = parseFloat(filterValue);
            shouldShow = marketCap >= threshold;
        }
        
        if (shouldShow) {
            row.style.display = '';
            displayedCount++;
        } else {
            row.style.display = 'none';
        }
    });
    
    // Update the displayed companies count
    document.getElementById('displayedCompanies').textContent = displayedCount;
}

// Initialize filter on page load
document.addEventListener('DOMContentLoaded', function() {
    filterByMarketCap();
});