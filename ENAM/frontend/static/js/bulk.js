$(document).ready(() => {
  loadDealsTable({
    id: "#bulkDealsTable",
    apiEndpoint: "/api/bulk_deals",
    dateField: "deal_date",
    type: "bulk"
  });
});