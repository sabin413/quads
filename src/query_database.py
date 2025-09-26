import sqlite3, pickle, pathlib

db = "/home/sadhika8/JupyterLinks/nobackup/quads_database/monthly_aggregated_digests_percentiles.db"
with sqlite3.connect(db) as conn:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM geosfp;")
    print("rows:", cur.fetchone()[0])

    cur.execute("SELECT model, year, month, id_string, percentile FROM geosfp LIMIT 1;")
    row = cur.fetchone()
    if row:
        qvals, qlist = pickle.loads(row[4])
        print("sample:", row[0], row[1], row[2], row[3], "q0:", qlist[0], "->", qvals[0])

