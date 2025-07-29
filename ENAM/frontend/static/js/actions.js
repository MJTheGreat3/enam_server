let allData = [];
let choicesInstance;

function titleCase(str) {
  return (str || "")
    .toLowerCase()
    .split(/[\s_]+/)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function formatDateDisplay(dateStr) {
  if (!dateStr) return "";
  const date = new Date(dateStr);
  if (isNaN(date)) return "";
  const day = String(date.getDate()).padStart(2, '0');
  const month = date.toLocaleString('default', { month: 'short' });
  const year = String(date.getFullYear()).slice(-2);
  return `${day} ${month} ${year}`;
}

function generateTooltip(row) {
  const tooltipFields = {
    "Symbol Name": row.symbol,
    "Record Date": formatDateDisplay(row.record_date),
    "Ex Date": formatDateDisplay(row.ex_date),
    "BC Start Date": row.bc_start_date,
    "BC End Date": row.bc_end_date,
    "ND Start Date": row.nd_start_date,
    "ND End Date": row.nd_end_date,
    "Actual Payment Date": row.actual_payment_date
  };

  return Object.entries(tooltipFields)
    .filter(([_, val]) => val)
    .map(([key, val]) => `${titleCase(key)}: ${formatDateDisplay(val) || val}`)
    .join("\n");
}

function renderGrid(data) {
  const grid = document.getElementById('grid');
  grid.innerHTML = "";

  if (data.length === 0) {
    grid.innerHTML = `<p style="text-align:center; color:#888;">No records match your filters.</p>`;
    return;
  }

  data.forEach(row => {
    const badge = document.createElement('div');
    badge.className = 'badge';
    badge.setAttribute('title', generateTooltip(row));

    badge.innerHTML = `
      <div class="badge-title">${row.security_name}</div>
      <div class="badge-text">Ex Date: ${formatDateDisplay(row.ex_date)}</div>
      <div class="badge-text">Record Date: ${formatDateDisplay(row.record_date)}</div>
      <div class="badge-text">${titleCase(row.purpose)}</div>
    `;

    grid.appendChild(badge);
  });
}

function populateSecurityFilter(data) {
  const uniqueSecurities = [...new Set(data.map(row => row.security_name).filter(Boolean))].sort();
  const selectEl = document.getElementById('securityFilter');
  selectEl.innerHTML = "";

  uniqueSecurities.forEach(name => {
    const option = document.createElement('option');
    option.value = name;
    option.textContent = titleCase(name);
    selectEl.appendChild(option);
  });

  if (choicesInstance) choicesInstance.destroy();
  choicesInstance = new Choices(selectEl, {
    removeItemButton: true,
    searchEnabled: true,
    shouldSort: false,
    placeholderValue: 'Select securities...'
  });
}

function applyFilters() {
  const selected = choicesInstance.getValue(true).map(s => s.toLowerCase());
  const exDateVal = document.getElementById('exDateFilter').value;
  const recordDateVal = document.getElementById('recordDateFilter').value;

  const filtered = allData.filter(row => {
    const secName = (row.security_name || "").toLowerCase();
    const exDate = row.ex_date?.slice(0, 10);
    const recDate = row.record_date?.slice(0, 10);

    const secMatch = selected.length === 0 || selected.includes(secName);
    const exMatch = !exDateVal || exDate === exDateVal;
    const recMatch = !recordDateVal || recDate === recordDateVal;

    return secMatch && exMatch && recMatch;
  });

  renderGrid(filtered);
}

function populateDateFilters(data) {
  const exFilter = document.getElementById('exDateFilter');
  const recFilter = document.getElementById('recordDateFilter');

  const exDates = [...new Set(data.map(row => row.ex_date?.slice(0, 10)).filter(Boolean))].sort();
  const recDates = [...new Set(data.map(row => row.record_date?.slice(0, 10)).filter(Boolean))].sort();

  exFilter.innerHTML = `<option value="">All Ex Dates</option>` +
    exDates.map(date => `<option value="${date}">${formatDateDisplay(date)}</option>`).join('');

  recFilter.innerHTML = `<option value="">All Record Dates</option>` +
    recDates.map(date => `<option value="${date}">${formatDateDisplay(date)}</option>`).join('');
}

document.addEventListener('DOMContentLoaded', async () => {
  try {
    const response = await fetch('/api/corp_actions');
    allData = await response.json();

    populateSecurityFilter(allData);
    populateDateFilters(allData);
    renderGrid(allData);

    document.getElementById('securityFilter').addEventListener('change', applyFilters);
    document.getElementById('exDateFilter').addEventListener('change', applyFilters);
    document.getElementById('recordDateFilter').addEventListener('change', applyFilters);
  } catch (err) {
    console.error('Error loading data:', err);
  }
});
