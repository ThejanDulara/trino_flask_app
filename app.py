import os
import time
import threading
import logging
from collections import defaultdict

from flask import Flask, jsonify, render_template, request
from trino.dbapi import connect
from trino.exceptions import TrinoExternalError, TrinoUserError, HttpError

# -------------------------------------------------
# Environment
# -------------------------------------------------
TRINO_HOST = os.getenv("TRINO_HOST", "127.0.0.1")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "web")
MYSQL_SCHEMA = os.getenv("MYSQL_SCHEMA", "crm_1")

# Cap concurrent Trino calls from this process
TRINO_MAX_CONCURRENCY = int(os.getenv("TRINO_MAX_CONCURRENCY", "2"))

# Cache TTLs (seconds)
DASHBOARD_CACHE_TTL = int(os.getenv("DASHBOARD_CACHE_TTL", "60"))
SMALL_CACHE_TTL = int(os.getenv("SMALL_CACHE_TTL", "60"))

# How many segments to show on bar/pie lists
SEGMENT_LIMIT = int(os.getenv("SEGMENT_LIMIT", "8"))

app = Flask(__name__)
log = app.logger
log.setLevel(logging.INFO)

_gate = threading.BoundedSemaphore(value=TRINO_MAX_CONCURRENCY)
_cache = {}  # key -> {"t": ts, "v": value}


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def trino_conn():
    # Fail fast under load
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
        max_attempts=1,
        request_timeout=10.0,
    )


def run_query(sql: str):
    try:
        with _gate:
            with trino_conn() as conn:
                cur = conn.cursor()
                cur.execute(sql)
                return cur.fetchall()
    except (TrinoUserError, TrinoExternalError, HttpError) as e:
        # Avoid log spam; keep succinct
        name = getattr(e, "error_name", e.__class__.__name__)
        log.warning("Trino error: %s", name)
        raise
    except Exception as e:
        log.error("General error talking to Trino: %r", e)
        raise


def cache_ttl(key: str, ttl: int, fn):
    now = time.time()
    entry = _cache.get(key)
    if entry and (now - entry["t"] < ttl):
        return entry["v"]
    v = fn()
    _cache[key] = {"t": now, "v": v}
    return v


# ------------------ Query builders (12-month window) ------------------
WINDOW_FILTER = "o.orderdate >= date_add('month', -12, current_date)"

def compute_kpis():
    def _do():
        sql = f"""
        WITH totals AS (
            SELECT
                ROUND(SUM(o.totalprice), 2) AS total_revenue,
                COUNT(*)                    AS total_orders,
                ROUND(AVG(o.totalprice), 2) AS avg_order_value
            FROM tpch.tiny.orders o
            WHERE {WINDOW_FILTER}
        ),
        top_seg AS (
            SELECT v.segment, SUM(o.totalprice) AS rev
            FROM tpch.tiny.orders o
            JOIN tpch.tiny.customer c ON o.custkey = c.custkey
            JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
            WHERE {WINDOW_FILTER}
            GROUP BY v.segment
            ORDER BY rev DESC
            LIMIT 1
        )
        SELECT t.total_revenue, t.total_orders, t.avg_order_value, s.segment AS top_segment
        FROM totals t
        CROSS JOIN top_seg s
        """
        row = run_query(sql)[0]
        return {
            "total_revenue": float(row[0]),
            "total_orders": int(row[1]),
            "avg_order_value": float(row[2]),
            "top_segment": row[3],
        }
    return cache_ttl("kpis", SMALL_CACHE_TTL, _do)


def compute_revenue_by_segment():
    def _do():
        sql = f"""
        SELECT v.segment, ROUND(SUM(o.totalprice), 2) AS revenue
        FROM tpch.tiny.orders o
        JOIN tpch.tiny.customer c ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        WHERE {WINDOW_FILTER}
        GROUP BY v.segment
        ORDER BY revenue DESC
        LIMIT {SEGMENT_LIMIT}
        """
        rows = run_query(sql)
        return {"labels": [r[0] for r in rows], "values": [float(r[1]) for r in rows]}
    return cache_ttl("revenue_by_segment", SMALL_CACHE_TTL, _do)


def compute_avg_order_value_by_segment():
    def _do():
        sql = f"""
        SELECT v.segment, ROUND(AVG(o.totalprice), 2) AS avg_order_value
        FROM tpch.tiny.orders o
        JOIN tpch.tiny.customer c ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        WHERE {WINDOW_FILTER}
        GROUP BY v.segment
        ORDER BY avg_order_value DESC
        LIMIT {SEGMENT_LIMIT}
        """
        rows = run_query(sql)
        return {"labels": [r[0] for r in rows], "values": [float(r[1]) for r in rows]}
    return cache_ttl("avg_order_value_by_segment", SMALL_CACHE_TTL, _do)


def compute_orders_count_by_segment():
    def _do():
        sql = f"""
        SELECT v.segment, COUNT(*) AS orders
        FROM tpch.tiny.orders o
        JOIN tpch.tiny.customer c ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        WHERE {WINDOW_FILTER}
        GROUP BY v.segment
        ORDER BY orders DESC
        LIMIT {SEGMENT_LIMIT}
        """
        rows = run_query(sql)
        return {"labels": [r[0] for r in rows], "values": [int(r[1]) for r in rows]}
    return cache_ttl("orders_count_by_segment", SMALL_CACHE_TTL, _do)


def compute_monthly_revenue_by_segment():
    def _do():
        # already limited to 12 months historically, keep as-is
        sql = f"""
        WITH bounds AS (SELECT max(orderdate) AS maxd FROM tpch.tiny.orders)
        SELECT date_trunc('month', o.orderdate) AS m,
               v.segment,
               ROUND(SUM(o.totalprice), 2) AS revenue
        FROM tpch.tiny.orders o
        JOIN tpch.tiny.customer c ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        CROSS JOIN bounds b
        WHERE o.orderdate >= date_add('month', -12, b.maxd)
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
        rows = run_query(sql)
        months = sorted({str(r[0])[:7] for r in rows})  # 'YYYY-MM'
        by_seg = defaultdict(lambda: {m: 0.0 for m in months})
        for m, seg, rev in rows:
            key = str(m)[:7]
            by_seg[seg][key] = float(rev)
        datasets = [{"label": seg, "data": [by_seg[seg][m] for m in months]} for seg in by_seg.keys()]
        return {"labels": months, "datasets": datasets}
    return cache_ttl("monthly_revenue_by_segment", SMALL_CACHE_TTL, _do)


def compute_top_customers(limit: int):
    key = f"top_customers_{limit}"
    def _do():
        sql = f"""
        SELECT c.name AS customer_name,
               v.segment,
               COUNT(o.orderkey)            AS orders,
               ROUND(SUM(o.totalprice), 2)  AS revenue
        FROM tpch.tiny.customer c
        JOIN tpch.tiny.orders   o ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        WHERE {WINDOW_FILTER}
        GROUP BY c.name, v.segment
        ORDER BY revenue DESC
        LIMIT {limit}
        """
        rows = run_query(sql)
        return [{"customer_name": r[0], "segment": r[1], "orders": int(r[2]), "revenue": float(r[3])} for r in rows]
    return cache_ttl(key, SMALL_CACHE_TTL, _do)


# -------------------------------------------------
# Routes
# -------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.get("/api/health")
def health():
    try:
        run_query("SELECT 1")
        return jsonify({"ok": True})
    except Exception:
        return jsonify({"ok": False}), 503


# ---- Individual endpoints (kept; now cached & windowed) ----
@app.get("/api/kpis")
def kpis():
    try:
        return jsonify(compute_kpis())
    except Exception as e:
        log.warning("kpis error: %r", e)
        return jsonify({}), 200


@app.get("/api/revenue_by_segment")
def revenue_by_segment():
    try:
        return jsonify(compute_revenue_by_segment())
    except Exception as e:
        log.warning("revenue_by_segment error: %r", e)
        return jsonify({"labels": [], "values": []}), 200


@app.get("/api/revenue_share_by_segment")
def revenue_share_by_segment():
    # Same payload as revenue_by_segment (client visualizes as share)
    return revenue_by_segment()


@app.get("/api/avg_order_value_by_segment")
def avg_order_value_by_segment():
    try:
        return jsonify(compute_avg_order_value_by_segment())
    except Exception as e:
        log.warning("avg_order_value_by_segment error: %r", e)
        return jsonify({"labels": [], "values": []}), 200


@app.get("/api/orders_count_by_segment")
def orders_count_by_segment():
    try:
        return jsonify(compute_orders_count_by_segment())
    except Exception as e:
        log.warning("orders_count_by_segment error: %r", e)
        return jsonify({"labels": [], "values": []}), 200


@app.get("/api/monthly_revenue_by_segment")
def monthly_revenue_by_segment():
    try:
        return jsonify(compute_monthly_revenue_by_segment())
    except Exception as e:
        log.warning("monthly_revenue_by_segment error: %r", e)
        return jsonify({"labels": [], "datasets": []}), 200


@app.get("/api/top_customers")
def top_customers():
    limit = int(request.args.get("limit", 20))
    try:
        rows = compute_top_customers(limit)
        return jsonify({"rows": rows})
    except Exception as e:
        log.warning("top_customers error: %r", e)
        return jsonify({"rows": []}), 200


# ---- Aggregated dashboard ----
@app.get("/api/dashboard")
def dashboard():
    try:
        data = cache_ttl("dashboard", DASHBOARD_CACHE_TTL, lambda: {
            "kpis": compute_kpis(),
            "revenue_by_segment": compute_revenue_by_segment(),
            "avg_order_value_by_segment": compute_avg_order_value_by_segment(),
            "orders_count_by_segment": compute_orders_count_by_segment(),
            "monthly_revenue_by_segment": compute_monthly_revenue_by_segment(),
        })
        return jsonify(data)
    except Exception as e:
        log.warning("dashboard aggregation error: %r", e)
        return jsonify({"error": "trino_busy"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    log.info(
        "Starting dev server; TRINO_HOST=%s TRINO_PORT=%s MYSQL_SCHEMA=%s CONC=%s",
        TRINO_HOST, TRINO_PORT, MYSQL_SCHEMA, TRINO_MAX_CONCURRENCY
    )
    app.run(host="0.0.0.0", port=port, debug=False)
