import os
import json
import time
import threading
import logging
from collections import defaultdict

from flask import Flask, jsonify, render_template, request
from trino.dbapi import connect
from trino.exceptions import TrinoExternalError, TrinoUserError, HttpError

# ---------------- Env & App ----------------
TRINO_HOST = os.getenv("TRINO_HOST", "127.0.0.1")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "web")
MYSQL_SCHEMA = os.getenv("MYSQL_SCHEMA", "crm_1")

# Concurrency gate to avoid hammering Trino from this process
TRINO_MAX_CONCURRENCY = int(os.getenv("TRINO_MAX_CONCURRENCY", "4"))

# Cache TTL (seconds)
DASHBOARD_CACHE_TTL = int(os.getenv("DASHBOARD_CACHE_TTL", "30"))
SMALL_CACHE_TTL = int(os.getenv("SMALL_CACHE_TTL", "30"))

app = Flask(__name__)
log = app.logger
log.setLevel(logging.INFO)

_gate = threading.BoundedSemaphore(value=TRINO_MAX_CONCURRENCY)
_cache = {}  # simple in-memory cache: key -> {"t": timestamp, "v": value}


# ---------------- Utilities ----------------
def trino_conn():
    # Use short timeouts & fewer retries so we fail fast under load
    # These kwargs are supported by trino's client in recent versions.
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
        max_attempts=1,
        request_timeout=10.0,
    )


def run_query(sql):
    try:
        with _gate:  # cap concurrent in-flight queries
            with trino_conn() as conn:
                cur = conn.cursor()
                cur.execute(sql)
                rows = cur.fetchall()
                # If description exists, you can also extract column names if needed
                return rows
    except (TrinoUserError, TrinoExternalError, HttpError) as e:
        # Log once per error type without flooding
        log.warning("Trino error: %s", getattr(e, "error_name", repr(e)))
        raise
    except Exception as e:
        log.error("General error talking to Trino: %r", e)
        raise


def cache_ttl(key, ttl, fn):
    now = time.time()
    entry = _cache.get(key)
    if entry and now - entry["t"] < ttl:
        return entry["v"]
    v = fn()
    _cache[key] = {"t": now, "v": v}
    return v


# ---------------- Routes ----------------
@app.route("/")
def index():
    # Assumes templates/index.html exists in your image (same as your working setup)
    return render_template("index.html")


@app.get("/api/health")
def health():
    try:
        run_query("SELECT 1")
        return jsonify({"ok": True})
    except Exception:
        return jsonify({"ok": False}), 503


# ---- Original small endpoints kept (now cached) ----
@app.get("/api/kpis")
def kpis():
    def compute():
        sql = f"""
        WITH totals AS (
            SELECT
                ROUND(SUM(o.totalprice), 2) AS total_revenue,
                COUNT(*)                    AS total_orders,
                ROUND(AVG(o.totalprice), 2) AS avg_order_value
            FROM tpch.tiny.orders o
        ),
        top_seg AS (
            SELECT v.segment, SUM(o.totalprice) AS rev
            FROM tpch.tiny.orders o
            JOIN tpch.tiny.customer c ON o.custkey = c.custkey
            JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
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

    data = cache_ttl("kpis", SMALL_CACHE_TTL, compute)
    return jsonify(data)


@app.get("/api/revenue_by_segment")
def revenue_by_segment():
    def compute():
        sql = f"""
        SELECT v.segment, ROUND(SUM(o.totalprice), 2) AS revenue
        FROM tpch.tiny.orders o
        JOIN tpch.tiny.customer c ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        GROUP BY v.segment
        ORDER BY revenue DESC
        """
        rows = run_query(sql)
        return {"labels": [r[0] for r in rows], "values": [float(r[1]) for r in rows]}

    data = cache_ttl("revenue_by_segment", SMALL_CACHE_TTL, compute)
    return jsonify(data)


@app.get("/api/revenue_share_by_segment")
def revenue_share_by_segment():
    # Same query as revenue_by_segment (client can treat it as shares visually)
    return revenue_by_segment()


@app.get("/api/monthly_revenue_by_segment")
def monthly_revenue_by_segment():
    def compute():
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

    data = cache_ttl("monthly_revenue_by_segment", SMALL_CACHE_TTL, compute)
    return jsonify(data)


@app.get("/api/top_customers")
def top_customers():
    limit = int(request.args.get("limit", 20))

    def compute():
        sql = f"""
        SELECT c.name AS customer_name, v.segment, COUNT(o.orderkey) AS orders, ROUND(SUM(o.totalprice), 2) AS revenue
        FROM tpch.tiny.customer c
        JOIN tpch.tiny.orders   o ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        GROUP BY c.name, v.segment
        ORDER BY revenue DESC
        LIMIT {limit}
        """
        rows = run_query(sql)
        return [{"customer_name": r[0], "segment": r[1], "orders": int(r[2]), "revenue": float(r[3])} for r in rows]

    key = f"top_customers_{limit}"
    data = cache_ttl(key, SMALL_CACHE_TTL, compute)
    return jsonify({"rows": data})


@app.get("/api/avg_order_value_by_segment")
def avg_order_value_by_segment():
    def compute():
        sql = f"""
        SELECT v.segment, ROUND(AVG(o.totalprice), 2) AS avg_order_value
        FROM tpch.tiny.orders o
        JOIN tpch.tiny.customer c ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        GROUP BY v.segment
        ORDER BY avg_order_value DESC
        """
        rows = run_query(sql)
        return {"labels": [r[0] for r in rows], "values": [float(r[1]) for r in rows]}

    data = cache_ttl("avg_order_value_by_segment", SMALL_CACHE_TTL, compute)
    return jsonify(data)


@app.get("/api/orders_count_by_segment")
def orders_count_by_segment():
    def compute():
        sql = f"""
        SELECT v.segment, COUNT(*) AS orders
        FROM tpch.tiny.orders o
        JOIN tpch.tiny.customer c ON o.custkey = c.custkey
        JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
        GROUP BY v.segment
        ORDER BY orders DESC
        """
        rows = run_query(sql)
        return {"labels": [r[0] for r in rows], "values": [int(r[1]) for r in rows]}

    data = cache_ttl("orders_count_by_segment", SMALL_CACHE_TTL, compute)
    return jsonify(data)


# ---- New: single, cached dashboard endpoint ----
@app.get("/api/dashboard")
def dashboard():
    try:
        # Use the cached sub-computations so this is very cheap under load
        data = {
            "kpis": kpis().json,  # reuse existing route logic via Response.json
            "revenue_by_segment": revenue_by_segment().json,
            "avg_order_value_by_segment": avg_order_value_by_segment().json,
            "orders_count_by_segment": orders_count_by_segment().json,
            "monthly_revenue_by_segment": monthly_revenue_by_segment().json,
        }
        return jsonify(data)
    except Exception as e:
        log.warning("dashboard aggregation error: %r", e)
        return jsonify({"error": "trino_busy"}), 200


if __name__ == "__main__":
    # Dev only. In containers we use gunicorn (see Dockerfile).
    port = int(os.getenv("PORT", "5000"))
    log.info(
        "Starting dev server; TRINO_HOST=%s TRINO_PORT=%s MYSQL_SCHEMA=%s CONC=%s",
        TRINO_HOST, TRINO_PORT, MYSQL_SCHEMA, TRINO_MAX_CONCURRENCY
    )
    app.run(host="0.0.0.0", port=port, debug=False)
