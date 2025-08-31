// public/app.js

let barSegmentChart, pieShareChart, lineMonthlyChart, barAOVSegmentChart, barOrdersSegmentChart;

async function fetchJSON(url, opts = {}) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

function fmtNumber(n) {
  const num = Number(n);
  if (Number.isNaN(num)) return "—";
  return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

// ---------- KPIs ----------
function renderKpis(k) {
  document.getElementById("kpiTotalRevenue").textContent = fmtNumber(k.total_revenue);
  document.getElementById("kpiTotalOrders").textContent = fmtNumber(k.total_orders);
  document.getElementById("kpiAvgOrderValue").textContent = fmtNumber(k.avg_order_value);
  document.getElementById("kpiTopSegment").textContent = k.top_segment || "—";
}

// ---------- Chart helpers ----------
function destroyChart(chart) {
  if (chart && typeof chart.destroy === "function") chart.destroy();
}

// Bar: revenue by segment
function renderBarSegment(data) {
  const ctx = document.getElementById("barSegment").getContext("2d");
  destroyChart(barSegmentChart);
  barSegmentChart = new Chart(ctx, {
    type: "bar",
    data: { labels: data.labels, datasets: [{ label: "Revenue", data: data.values }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } }
    }
  });
}

// Pie: share by segment
function renderPieShare(data) {
  const ctx = document.getElementById("pieShare").getContext("2d");
  destroyChart(pieShareChart);
  pieShareChart = new Chart(ctx, {
    type: "pie",
    data: { labels: data.labels, datasets: [{ data: data.values }] },
    options: { responsive: true }
  });
}

// Bar: average order value by segment
function renderBarAOVSegment(data) {
  const ctx = document.getElementById("barAOVSegment").getContext("2d");
  destroyChart(barAOVSegmentChart);
  barAOVSegmentChart = new Chart(ctx, {
    type: "bar",
    data: { labels: data.labels, datasets: [{ label: "Avg Order Value", data: data.values }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
  });
}

// Bar: orders count by segment
function renderBarOrdersSegment(data) {
  const ctx = document.getElementById("barOrdersSegment").getContext("2d");
  destroyChart(barOrdersSegmentChart);
  barOrdersSegmentChart = new Chart(ctx, {
    type: "bar",
    data: { labels: data.labels, datasets: [{ label: "Orders", data: data.values }] },
    options: { responsive: true, plugins: { legend: { display: false } } }
  });
}

// Line: monthly revenue by segment
function renderLineMonthly(data) {
  const ctx = document.getElementById("lineMonthly").getContext("2d");
  destroyChart(lineMonthlyChart);
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

// Top customers table (kept separate for user-controlled limit)
async function loadTopCustomers(limit = 5) {
  const data = await fetchJSON(`/api/top_customers?limit=${limit}`);
  const tbody = document.querySelector("#customersTable tbody");
  tbody.innerHTML = "";
  (data.rows || []).forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.customer_name}</td>
      <td>${r.segment}</td>
      <td style="text-align:right">${fmtNumber(r.orders)}</td>
      <td style="text-align:right">${fmtNumber(r.revenue)}</td>
    `;
    tbody.appendChild(tr);
  });
}

async function initDashboard() {
  try {
    // Single call to get KPIs + small charts
    const dash = await fetchJSON("/api/dashboard");
    if (dash.error) {
      console.error("Backend reported error:", dash.error);
      return; // Keep UI blank instead of spinning
    }

    renderKpis(dash.kpis || {});

    // Charts (all small, already pre-aggregated and cached server-side)
    if (dash.revenue_by_segment) {
      renderBarSegment(dash.revenue_by_segment);
      renderPieShare(dash.revenue_by_segment); // reuse the same data for share
    }
    if (dash.avg_order_value_by_segment) renderBarAOVSegment(dash.avg_order_value_by_segment);
    if (dash.orders_count_by_segment) renderBarOrdersSegment(dash.orders_count_by_segment);
    if (dash.monthly_revenue_by_segment) renderLineMonthly(dash.monthly_revenue_by_segment);

    // Initial top customers (default 5)
    await loadTopCustomers(5);
  } catch (e) {
    console.error(e);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  await initDashboard();

  document.getElementById("refreshBtn").addEventListener("click", async () => {
    const limit = parseInt(document.getElementById("limitInput").value || "20", 10);
    await loadTopCustomers(limit);
  });
});
