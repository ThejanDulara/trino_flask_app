let barSegmentChart, pieShareChart, lineMonthlyChart, barAOVSegmentChart, barOrdersSegmentChart;

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// ---------- KPIs ----------
async function loadKpis() {
  const k = await fetchJSON("/api/kpis");
  // Format numbers nicely
  const fmt = (n) => Number(n).toLocaleString(undefined, { maximumFractionDigits: 2 });
  document.getElementById("kpiTotalRevenue").textContent = fmt(k.total_revenue);
  document.getElementById("kpiTotalOrders").textContent = fmt(k.total_orders);
  document.getElementById("kpiAvgOrderValue").textContent = fmt(k.avg_order_value);
  document.getElementById("kpiTopSegment").textContent = k.top_segment || "—";
}

// ---------- Charts ----------
async function loadBarSegment() {
  const res = await fetch("/api/revenue_by_segment");
  const data = await res.json(); // { labels: [...], values: [...] }

  const ctx = document.getElementById("barSegment").getContext("2d");
  barSegmentChart = new Chart(ctx, {
    type: "bar",
    data: { labels: data.labels, datasets: [{ label: "Revenue", data: data.values }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
  });

  return data; // <-- we’ll reuse this for the pie
}

function renderPieFrom(data) {
  const ctx = document.getElementById("pieSegment").getContext("2d");
  pieSegmentChart = new Chart(ctx, {
    type: "pie",
    data: { labels: data.labels, datasets: [{ data: data.values }] },
    options: { responsive: true }
  });
}

async function loadPieShare() {
  const data = await fetchJSON("/api/revenue_share_by_segment");
  const ctx = document.getElementById("pieShare");
  if (pieShareChart) pieShareChart.destroy();
  pieShareChart = new Chart(ctx, {
    type: "pie",
    data: { labels: data.labels, datasets: [{ data: data.values }] },
    options: { responsive: true }
  });
}

async function loadBarAOVSegment() {
  const data = await fetchJSON("/api/avg_order_value_by_segment");
  const ctx = document.getElementById("barAOVSegment");
  if (barAOVSegmentChart) barAOVSegmentChart.destroy();
  barAOVSegmentChart = new Chart(ctx, {
    type: "bar",
    data: { labels: data.labels, datasets: [{ label: "Avg Order Value", data: data.values }] },
    options: { responsive: true,maintainAspectRatio: false, plugins: { legend: { display: false } } }
  });
}

async function loadBarOrdersSegment() {
  const data = await fetchJSON("/api/orders_count_by_segment");
  const ctx = document.getElementById("barOrdersSegment");
  if (barOrdersSegmentChart) barOrdersSegmentChart.destroy();
  barOrdersSegmentChart = new Chart(ctx, {
    type: "bar",
    data: { labels: data.labels, datasets: [{ label: "Orders", data: data.values }] },
    options: { responsive: true, plugins: { legend: { display: false } } }
  });
}

async function loadLineMonthly() {
  const data = await fetchJSON("/api/monthly_revenue_by_segment");
  const ctx = document.getElementById("lineMonthly");
  if (lineMonthlyChart) lineMonthlyChart.destroy();
  lineMonthlyChart = new Chart(ctx, {
    type: "line",
    data: { labels: data.labels, datasets: data.datasets },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "top" } },
      scales: { y: { beginAtZero: true } }
    }
  });
}

async function loadTopCustomers(limit=5) {
  const data = await fetchJSON(`/api/top_customers?limit=${limit}`);
  const tbody = document.querySelector("#customersTable tbody");
  tbody.innerHTML = "";
  data.rows.forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.customer_name}</td>
      <td>${r.segment}</td>
      <td style="text-align:right">${r.orders}</td>
      <td style="text-align:right">${r.revenue.toLocaleString()}</td>
    `;
    tbody.appendChild(tr);
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  await Promise.all([
    loadKpis(),
    loadBarSegment(),
    loadPieShare(),
    loadBarAOVSegment(),
    loadBarOrdersSegment(),
    loadLineMonthly(),
    loadTopCustomers()
  ]);

  document.getElementById("refreshBtn").addEventListener("click", async () => {
    const limit = parseInt(document.getElementById("limitInput").value || "20", 10);
    await loadTopCustomers(limit);
  });
});
