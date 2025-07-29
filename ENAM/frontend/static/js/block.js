$(document).ready(() => {
  loadDealsTable({
    id: "#blockDealsTable",
    apiEndpoint: "/api/block_deals",
    dateField: "deal_date",
    type: "block"
  });
});
