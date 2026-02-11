import sqlite3, pickle, pathlib

db = "/home/sadhika8/JupyterLinks/nobackup/quads_database/geosit_monthly_aggregated_centroids_and_quantiles.db"
with sqlite3.connect(db) as conn:
    cur = conn.cursor()
    #cur.execute("SELECT COUNT(*) FROM geosfp;")
    #print("rows:", cur.fetchone()[0])

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = [row[0] for row in cur.fetchall()]

    print("tables:", tables)

    for t in tables:
        cur.execute(f"PRAGMA table_info({t});")
        cols = [row[1] for row in cur.fetchall()]  # row[1] is column name
        print(f"{t} columns: {cols}")


