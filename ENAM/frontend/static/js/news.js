document.addEventListener('DOMContentLoaded', () => {
  const tbody = document.querySelector('#newsTable tbody');
  const sourceFilter = document.getElementById('sourceFilter');
  const categoryFilter = document.getElementById('categoryFilter');
  const searchInput = document.getElementById('searchInput');
  const resetBtn = document.getElementById('resetFilters');
  const pageSizeSelect = document.getElementById('pageSize');
  const paginationTop = document.getElementById('paginationTop');
  const paginationBottom = document.getElementById('paginationBottom');
  const scrollTopBtn = document.getElementById('scrollTopBtn');

  let allData = [];
  let filteredData = [];
  let currentPage = 1;
  let pageSize = parseInt(pageSizeSelect.value);

  fetch('/api/news')
    .then(response => response.json())
    .then(data => {
      allData = data
        .filter(r => r.time && r.headline)
        .sort((a, b) => new Date(b.time) - new Date(a.time));
      populateFilters();
      applyFilters();
    });

  function populateFilters() {
    const sources = new Set();
    const categories = new Set();

    allData.forEach(item => {
      if (item.source) sources.add(item.source.trim());
      if (item.category) categories.add(item.category.trim());
    });

    [...sources].sort().forEach(src => {
      sourceFilter.innerHTML += `<option value="${src}">${src}</option>`;
    });
    [...categories].sort().forEach(cat => {
      categoryFilter.innerHTML += `<option value="${cat}">${cat}</option>`;
    });
  }

  function getBadgeColor(category) {
    const map = {
      'markets': '#007bff',
      'economy': '#28a745',
      'finance': '#ffc107',
      'companies': '#17a2b8',
      'stocks': '#6610f2',
      'ipos': '#fd7e14',
      'industry': '#6c757d',
      'business': '#20c997',
      'commodities': '#dc3545',
    };

    return map[category.toLowerCase()] || '#6c757d';
  }

  function renderTable(data) {
    tbody.innerHTML = '';
    if (data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="4" class="text-center">No records found.</td></tr>';
      return;
    }

    const start = (currentPage - 1) * pageSize;
    const end = start + pageSize;
    const pageData = data.slice(start, end);
    const now = new Date();

    pageData.forEach(item => {
      const time = new Date(item.time);
      const diffMs = now - time;
      const diffMins = Math.floor(diffMs / 60000);
      const diffHrs = Math.floor(diffMins / 60);
      const diffDays = Math.floor(diffHrs / 24);

      let relativeTime = '';
      if (diffMins < 1) relativeTime = 'Just now';
      else if (diffMins < 60) relativeTime = `${diffMins} minute${diffMins === 1 ? '' : 's'} ago`;
      else if (diffHrs < 24) relativeTime = `${diffHrs} hour${diffHrs === 1 ? '' : 's'} ago`;
      else relativeTime = `${diffDays} day${diffDays === 1 ? '' : 's'} ago`;

      const categories = item.category
        ? item.category.split(',').map(cat => cat.trim()).filter(Boolean)
        : [];

      const badgesHTML = categories.map(cat => {
        const color = getBadgeColor(cat);
        return `<span class="badge rounded-pill me-1 mb-1" style="background-color: ${color}; color: #fff;">${cat}</span>`;
      }).join(' ');

      const row = document.createElement('tr');
      row.innerHTML = `
        <td colspan="4">
          <div class="d-flex justify-content-between align-items-start flex-wrap">
            <div>
              <div>${badgesHTML}</div>
              <div class="fs-5 fw-semibold mb-1">
                <a href="${item.link}" target="_blank" class="text-decoration-none">${item.headline}</a>
              </div>
              <div class="text-muted small">Source: ${item.source}</div>
            </div>
            <div class="text-muted small text-end">${relativeTime}</div>
          </div>
        </td>
      `;
      tbody.appendChild(row);
    });

    renderPaginationControls(data.length);
  }

  function renderPaginationControls(totalItems) {
    const totalPages = Math.ceil(totalItems / pageSize);
    if (totalPages <= 1) {
      paginationTop.innerHTML = '';
      paginationBottom.innerHTML = '';
      return;
    }

    const controlsHTML = generatePaginationHTML(totalPages);
    paginationTop.innerHTML = controlsHTML;
    paginationBottom.innerHTML = controlsHTML;

    document.querySelectorAll('.page-link').forEach(btn => {
      btn.addEventListener('click', () => {
        const target = btn.dataset.page;
        if (target === 'prev' && currentPage > 1) currentPage--;
        else if (target === 'next' && currentPage < totalPages) currentPage++;
        else if (!isNaN(target)) currentPage = parseInt(target);
        renderTable(filteredData);
        window.scrollTo({ top: 0, behavior: 'smooth' });
      });
    });
  }

  function generatePaginationHTML(totalPages) {
    let html = `<nav><ul class="pagination justify-content-center mb-0">`;

    html += `<li class="page-item ${currentPage === 1 ? 'disabled' : ''}">
              <button class="page-link" data-page="prev">&laquo;</button>
            </li>`;

    const pageButtons = [];
    if (totalPages <= 7) {
      for (let i = 1; i <= totalPages; i++) {
        pageButtons.push(renderPageButton(i));
      }
    } else {
      if (currentPage > 3) {
        pageButtons.push(renderPageButton(1));
        if (currentPage > 4) pageButtons.push(renderEllipsis());
      }

      const start = Math.max(1, currentPage - 2);
      const end = Math.min(totalPages, currentPage + 2);

      for (let i = start; i <= end; i++) {
        pageButtons.push(renderPageButton(i));
      }

      if (currentPage < totalPages - 2) {
        if (currentPage < totalPages - 3) pageButtons.push(renderEllipsis());
        pageButtons.push(renderPageButton(totalPages));
      }
    }

    html += pageButtons.join('');

    html += `<li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">
              <button class="page-link" data-page="next">&raquo;</button>
            </li>`;

    html += `</ul></nav>`;
    return html;
  }

  function renderPageButton(i) {
    return `<li class="page-item ${i === currentPage ? 'active' : ''}">
              <button class="page-link" data-page="${i}">${i}</button>
            </li>`;
  }

  function renderEllipsis() {
    return `<li class="page-item disabled">
              <span class="page-link">...</span>
            </li>`;
  }

  function applyFilters() {
    const src = sourceFilter.value.toLowerCase();
    const cat = categoryFilter.value.toLowerCase();
    const search = searchInput.value.toLowerCase();

    filteredData = allData.filter(item => {
      const srcMatch = !src || item.source.toLowerCase() === src;
      const catMatch = !cat || item.category.toLowerCase() === cat;
      const searchMatch = !search || item.headline.toLowerCase().includes(search);
      return srcMatch && catMatch && searchMatch;
    });

    currentPage = 1;
    renderTable(filteredData);
  }

  sourceFilter.addEventListener('change', applyFilters);
  categoryFilter.addEventListener('change', applyFilters);
  searchInput.addEventListener('input', applyFilters);
  pageSizeSelect.addEventListener('change', () => {
    pageSize = parseInt(pageSizeSelect.value);
    currentPage = 1;
    renderTable(filteredData);
  });

  resetBtn.addEventListener('click', () => {
    sourceFilter.value = '';
    categoryFilter.value = '';
    searchInput.value = '';
    pageSizeSelect.value = '50';
    pageSize = 50;
    currentPage = 1;
    applyFilters();
  });

  window.addEventListener('scroll', () => {
    if (document.body.scrollTop > 200 || document.documentElement.scrollTop > 200) {
      scrollTopBtn.style.display = 'block';
    } else {
      scrollTopBtn.style.display = 'none';
    }
  });

  scrollTopBtn.addEventListener('click', () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });
});
