$(document).ready(function () {
  loadTable({
    id: "#insiderTable",
    controlsId: "#insiderControls",
    apiEndpoint: "/api/insider",
    dateField: "time",
    columnMap: {
      stock: "Stock",
      name: "Name",
      clause: "Cluase",
      amount: "Amount",
      type: "Type",
      value: "Value",
      transaction: "Transaction",
      attachment: "Attachment",
      time: "Time"
    },
    nowrapColumns: ["Stock", "Amount", "Value", "Attachment", "Time"]
  });
});