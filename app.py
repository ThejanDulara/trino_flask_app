import os
from flask import Flask, jsonify, render_template, request
from trino.dbapi import connect
from collections import defaultdict

TRINO_HOST = os.getenv("TRINO_HOST", "127.0.0.1")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "web")
MYSQL_SCHEMA = os.getenv("MYSQL_SCHEMA", "crm_1")

app = Flask(__name__)

def trino_conn():
    return connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)

@app.route("/")
def index():
    return render_template("index.html")

@app.get("/api/kpis")
def kpis():
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
    with trino_conn() as conn:
        cur = conn.cursor(); cur.execute(sql)
        row = cur.fetchone()
    return jsonify({
        "total_revenue": float(row[0]),
        "total_orders": int(row[1]),
        "avg_order_value": float(row[2]),
        "top_segment": row[3]
    })

@app.get("/api/revenue_by_segment")
def revenue_by_segment():
    sql = f"""
    SELECT v.segment, ROUND(SUM(o.totalprice), 2) AS revenue
    FROM tpch.tiny.orders o
    JOIN tpch.tiny.customer c ON o.custkey = c.custkey
    JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
    GROUP BY v.segment
    ORDER BY revenue DESC
    """
    with trino_conn() as conn:
        cur = conn.cursor(); cur.execute(sql)
        rows = cur.fetchall()
    return jsonify({"labels": [r[0] for r in rows], "values": [float(r[1]) for r in rows]})

@app.get("/api/revenue_share_by_segment")
def revenue_share_by_segment():
    sql = f"""
    SELECT v.segment, ROUND(SUM(o.totalprice), 2) AS revenue
    FROM tpch.tiny.orders o
    JOIN tpch.tiny.customer c ON o.custkey = c.custkey
    JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
    GROUP BY v.segment
    ORDER BY revenue DESC
    """
    with trino_conn() as conn:
        cur = conn.cursor(); cur.execute(sql)
        rows = cur.fetchall()
    return jsonify({"labels": [r[0] for r in rows], "values": [float(r[1]) for r in rows]})

@app.get("/api/monthly_revenue_by_segment")
def monthly_revenue_by_segment():
    sql = f"""
    WITH bounds AS (
        SELECT max(orderdate) AS maxd
        FROM tpch.tiny.orders
    )
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
    with trino_conn() as conn:
        cur = conn.cursor(); cur.execute(sql)
        rows = cur.fetchall()

    months = sorted({str(r[0])[:7] for r in rows})  # 'YYYY-MM'
    by_seg = defaultdict(lambda: {m: 0.0 for m in months})
    for m, seg, rev in rows:
        key = str(m)[:7]
        by_seg[seg][key] = float(rev)
    datasets = [{"label": seg, "data": [by_seg[seg][m] for m in months]} for seg in by_seg.keys()]
    return jsonify({"labels": months, "datasets": datasets})

@app.get("/api/top_customers")
def top_customers():
    limit = int(request.args.get("limit", 5))
    sql = f"""
    SELECT c.name AS customer_name, v.segment, COUNT(o.orderkey) AS orders, ROUND(SUM(o.totalprice), 2) AS revenue
    FROM tpch.tiny.customer c
    JOIN tpch.tiny.orders   o ON o.custkey = c.custkey
    JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
    GROUP BY c.name, v.segment
    ORDER BY revenue DESC
    LIMIT {limit}
    """
    with trino_conn() as conn:
        cur = conn.cursor(); cur.execute(sql)
        rows = cur.fetchall()
    data = [{"customer_name": r[0], "segment": r[1], "orders": int(r[2]), "revenue": float(r[3])} for r in rows]
    return jsonify({"rows": data})

@app.get("/api/avg_order_value_by_segment")
def avg_order_value_by_segment():
    sql = f"""
    SELECT v.segment, ROUND(AVG(o.totalprice), 2) AS avg_order_value
    FROM tpch.tiny.orders o
    JOIN tpch.tiny.customer c ON o.custkey = c.custkey
    JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
    GROUP BY v.segment
    ORDER BY avg_order_value DESC
    """
    with trino_conn() as conn:
        cur = conn.cursor(); cur.execute(sql)
        rows = cur.fetchall()
    return jsonify({"labels": [r[0] for r in rows], "values": [float(r[1]) for r in rows]})

@app.get("/api/orders_count_by_segment")
def orders_count_by_segment():
    sql = f"""
    SELECT v.segment, COUNT(*) AS orders
    FROM tpch.tiny.orders o
    JOIN tpch.tiny.customer c ON o.custkey = c.custkey
    JOIN mysql.{MYSQL_SCHEMA}.vip_customers v ON v.custkey = c.custkey
    GROUP BY v.segment
    ORDER BY orders DESC
    """
    with trino_conn() as conn:
        cur = conn.cursor(); cur.execute(sql)
        rows = cur.fetchall()
    return jsonify({"labels": [r[0] for r in rows], "values": [int(r[1]) for r in rows]})

if __name__ == "__main__":
    # Railway gives you $PORT for the public HTTP port
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
