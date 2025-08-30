import pandas as pd
from trino.dbapi import connect

conn = connect(host='127.0.0.1', port=8080, user='demo')

# Check TPCH date range (to explain why the last-12-months filter was empty)
print(pd.read_sql("SELECT min(orderdate) AS min_d, max(orderdate) AS max_d FROM tpch.tiny.orders", conn))

# Sanity: see catalogs/schemas
print(pd.read_sql("SHOW CATALOGS", conn))
print(pd.read_sql("SHOW SCHEMAS FROM mysql", conn))
