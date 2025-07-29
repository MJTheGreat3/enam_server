// table_utils.js - updated to support PSQL fetch instead of CSV
function loadTable(config) {
  const { id, controlsId, apiEndpoint, dateField, columnMap, nowrapColumns = [] } = config;

  function parseDate(str) {
    if (!str) return null;
    try {
      return new Date(
        str.replace(/(\d{2})-([A-Za-z]{3})-(\d{4})/, (_, d, m, y) => {
          const months = {
            Jan: "01", Feb: "02", Mar: "03", Apr: "04", May: "05", Jun: "06",
            Jul: "07", Aug: "08", Sep: "09", Oct: "10", Nov: "11", Dec: "12"
          };
          return `${y}-${months[m]}-${d}`;
        })
      );
    } catch {
      return null;
    }
  }

  function filterData(data, range) {
    const now = new Date();
    const cutoff = new Date();

    if (range === "1day") cutoff.setDate(now.getDate() - 1);
    else if (range === "1week") cutoff.setDate(now.getDate() - 7);
    else if (range === "1month") cutoff.setMonth(now.getMonth() - 1);

    return data.filter((row) => {
      if (range === "all_time") return true;

      const date = parseDate(row[dateField]);
      if (!(date instanceof Date) || isNaN(date)) return false;
      return date >= cutoff;
    });
  }

  function renderDateButtons(onChange) {
    const btnGroup = $(
      `<div class="date-button-group mb-3 text-center">
        <button class="btn btn-outline-primary filter-btn" data-range="1day">1 Day</button>
        <button class="btn btn-outline-primary filter-btn" data-range="1week">1 Week</button>
        <button class="btn btn-outline-primary filter-btn" data-range="1month">1 Month</button>
        <button class="btn btn-outline-primary filter-btn" data-range="all_time">All Time</button>
      </div>`
    );

    btnGroup.on("click", "button", function () {
      btnGroup.find("button").removeClass("active");
      $(this).addClass("active");
      onChange($(this).data("range"));
    });

    btnGroup.find('[data-range="all_time"]').addClass("active");
    return btnGroup;
  }

  async function fetchData() {
    try {
      const response = await fetch(apiEndpoint);
      const data = await response.json();
      console.log(`[INFO] Loaded data from ${apiEndpoint}:`, data);
      return data;
    } catch (err) {
      console.error("[ERROR] Failed to fetch data:", err);
      return [];
    }
  }

  function renderAttachment(data) {
    if (!data) return '';
    return `
      <div style="text-align: center;">
        <a href="${data}" target="_blank">
          <img class="download-icon" src="/static/assets/img/download.svg" alt="Download" style="height:20px;width:20px;">
        </a>
      </div>`;
  }

  async function init() {
    const originalData = await fetchData();

    let currentRange = "all_time";
    $(controlsId).append(renderDateButtons((range) => {
      currentRange = range;
      update();
    }));

    function update() {
      const filtered = filterData(originalData, currentRange);

      let displayData;
      let columns;

      if (columnMap && Object.keys(columnMap).length > 0) {
        displayData = filtered.map(row => {
          const newRow = {};
          Object.keys(columnMap).forEach(key => {
            newRow[columnMap[key]] = row[key];
          });
          return newRow;
        });

        columns = Object.values(columnMap).map(col => {
          if (col === "Attachment") {
            return {
              title: col,
              data: col,
              className: "text-center",
              render: renderAttachment
            };
          }
          const cellClass = `text-center ${nowrapColumns.includes(col) ? 'nowrap-cell' : 'wrap-cell'}`;
          return { title: col, data: col, className: cellClass };
        });
      } else {
        displayData = [...filtered];
        const csvHeaders = Object.keys(originalData[0]);
        columns = csvHeaders.map(col => {
          if (col === "Attachment") {
            return {
              title: col,
              data: col,
              className: "text-center",
              render: renderAttachment
            };
          }
          const cellClass = `text-center ${nowrapColumns.includes(col) ? 'nowrap-cell' : 'wrap-cell'}`;
          return { title: col, data: col, className: cellClass };
        });
      }

      const timeIndex = columns.findIndex(c => c.title === "Time");

      if ($.fn.DataTable.isDataTable(id)) {
        $(id).DataTable().clear().rows.add(displayData).draw();
      } else {
        $.fn.dataTable.moment('DD-MMM-YYYY HH:mm:ss');
        $.fn.dataTable.moment('DD-MMM-YYYY HH:mm');

        $(id).DataTable({
          data: displayData,
          columns: columns,
          pageLength: 10,
          responsive: true,
          autoWidth: false,
          order: timeIndex >= 0 ? [[timeIndex, 'desc']] : [],
          createdRow: function(row) {
            $(row).addClass('text-center');
          },
          headerCallback: function(thead) {
            $(thead).find('th').addClass('text-center');
          }
        });
      }
    }

    update();
  }

  init();
}
