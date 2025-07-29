function timeAgo(dateString) {
  if (!dateString) return 'N/A';
  const now = new Date();
  const updated = new Date(dateString.replace(' ', 'T'));
  if (isNaN(updated.getTime())) return dateString;

  const diffSec = Math.floor((now - updated) / 1000);
  const diffMin = Math.floor(diffSec / 60);
  const diffHrs = Math.floor(diffMin / 60);
  const diffDays = Math.floor(diffHrs / 24);

  if (diffSec < 30) return "just now";
  if (diffSec < 60) return `${diffSec} seconds ago`;
  if (diffMin < 60) return `${diffMin} minute${diffMin === 1 ? '' : 's'} ago`;
  if (diffHrs < 24) return `${diffHrs} hour${diffHrs === 1 ? '' : 's'} ago`;
  if (diffDays === 1) return "yesterday";
  if (diffDays < 7) return `${diffDays} days ago`;

  return updated.toLocaleString();
}

function loadLastUpdated() {
  fetch('/api/last-updated-data')
    .then(res => res.json())
    .then(data => {
      const raw = data.last_updated_data || '';
      $("#lastUpdated").text(timeAgo(raw));
    })
    .catch(err => console.error("[ERROR] Fetching last updated:", err));
}

$(document).on("click", "#refreshBtn", function () {
  if (confirm("Run data refresh? This will fetch new data from sources.")) {
    fetch('/api/refresh-data-sync', { method: 'POST' })
      .then(res => res.json())
      .then(data => {
        alert(data.message);
        const raw = data.last_updated || '';
        $("#lastUpdated").text(timeAgo(raw));
      })
      .catch(err => alert("Error triggering refresh: " + err));
  }
});

function loadDealsTable({ id, apiEndpoint, dateField, type }) {
  let data = [];
  let currentRange = "all_time";
  let selectedExchange = "BOTH";

  const columnConfigs = {
    block: [
      { data: "source", title: "Source" },
      { data: "deal_date", title: "Deal Date" },
      { data: "security_name", title: "Security Name" },
      { data: "client_name", title: "Client Name", className: 'wrap-cell' },
      { data: "deal_type", title: "Deal Type" },
      { data: "quantity", title: "Quantity" },
      { data: "trade_price", title: "Trade Price" }
    ],
    bulk: [
      { data: "source", title: "Source" },
      { data: "deal_date", title: "Deal Date" },
      { data: "security_name", title: "Security Name" },
      { data: "client_name", title: "Client Name", className: 'wrap-cell' },
      { data: "deal_type", title: "Deal Type" },
      { data: "quantity", title: "Quantity" },
      { data: "price", title: "Price" }
    ]
  };

  const parseDate = (str) => {
    if (!str) return null;
    if (str.includes("/")) {
      const [d, m, y] = str.split("/");
      return new Date(`${y}-${m}-${d}`);
    }
    return new Date(str);
  };

  function filterData() {
    const now = new Date();
    let cutoff = new Date();
    if (currentRange === "1day") cutoff.setDate(now.getDate() - 2);
    else if (currentRange === "1week") cutoff.setDate(now.getDate() - 8);
    else if (currentRange === "1month") cutoff.setMonth(now.getMonth() - 1);

    return data.filter(row => {
      const date = parseDate(row[dateField]);
      const dateValid = currentRange === "all_time" || (date && date >= cutoff);
      const exchangeValid = !selectedExchange || selectedExchange === "BOTH" || row["source"] === selectedExchange;
      return dateValid && exchangeValid;
    });
  }

  function renderTable(data) {
    const columns = columnConfigs[type];

    if ($.fn.DataTable.isDataTable(id)) {
      const table = $(id).DataTable();
      table.clear().rows.add(data).draw();
    } else {
      $(id).DataTable({
        data: data,
        columns: columns,
        order: [[columns.findIndex(col => col.data === dateField), 'desc']],
        createdRow: row => $(row).addClass('text-center'),
        headerCallback: thead => $(thead).find('th').addClass('text-center')
      });
    }
  }

  const exchangeButtons = $(`
    <div class="exchange-button-group text-center mb-2">
      <button class="exchange-btn active" data-exchange="BOTH">Both</button>
      <button class="exchange-btn" data-exchange="NSE">NSE</button>
      <button class="exchange-btn" data-exchange="BSE">BSE</button>
    </div>
  `);
  exchangeButtons.on("click", ".exchange-btn", function () {
    exchangeButtons.find("button").removeClass("active");
    $(this).addClass("active");
    selectedExchange = $(this).data("exchange");
    updateFiltered();
  });
  $(id).before(exchangeButtons);

  const dateButtons = $(`
    <div class="date-button-group text-center mb-2">
      <button class="filter-btn" data-range="1day">1 Day</button>
      <button class="filter-btn" data-range="1week">1 Week</button>
      <button class="filter-btn" data-range="1month">1 Month</button>
      <button class="filter-btn active" data-range="all_time">All Time</button>
    </div>
  `);
  dateButtons.on("click", ".filter-btn", function () {
    dateButtons.find("button").removeClass("active");
    $(this).addClass("active");
    currentRange = $(this).data("range");
    updateFiltered();
  });
  $(id).before(dateButtons);

  function updateFiltered() {
    const filtered = filterData();
    renderTable(filtered);
  }

  fetch(apiEndpoint)
    .then(res => res.json())
    .then(json => {
      if (!Array.isArray(json)) throw new Error("Invalid data format");
      data = json.filter(row =>
        Object.values(row).some(cell => (cell || "").toString().trim() !== "")
      );
      updateFiltered();
    })
    .catch(err => console.error("[ERROR] Loading deals:", err));
}

document.addEventListener('DOMContentLoaded', () => {
  loadLastUpdated();

  const toggleDataBtn = document.getElementById('toggleDataRefresh');
  const toggleNewsBtn = document.getElementById('toggleNewsRefresh');

  if (toggleDataBtn && toggleNewsBtn) {
    let schedulerState = { data: true, news: true };

    async function fetchSchedulerStatus() {
      const res = await fetch('/api/scheduler');
      const json = await res.json();
      schedulerState = json;
      updateButtonLabels();
    }

    function updateButtonLabels() {
      toggleDataBtn.textContent = schedulerState.data ? 'Disable Data Refresh' : 'Enable Data Refresh';
      toggleNewsBtn.textContent = schedulerState.news ? 'Disable News Refresh' : 'Enable News Refresh';
    }

    async function toggleScheduler(key) {
      const enable = !schedulerState[key];
      await fetch('/api/scheduler', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key, enable })
      });
      schedulerState[key] = enable;
      updateButtonLabels();
    }

    toggleDataBtn.addEventListener('click', () => toggleScheduler('data'));
    toggleNewsBtn.addEventListener('click', () => toggleScheduler('news'));

    fetchSchedulerStatus();
  }
});
