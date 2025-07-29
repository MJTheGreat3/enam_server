$(document).ready(function () {
  loadTable({
    id: "#announcementTable",
    controlsId: "#announcementControls",
    apiEndpoint: "/api/announcements",
    dateField: "time",
    columnMap: {
      stock: "Stock",
      subject: "Subject",
      announcement: "Announcement",
      attachment: "Attachment",
      time: "Time"
    },
    nowrapColumns: ["Stock", "Attachment", "Time"]
  });
});
