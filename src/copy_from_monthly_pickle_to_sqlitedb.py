from __future__ import annotations

from pathlib import Path
import sqlite3
import pickle


def load_monthly_pickle(base_out_dir: str, model: str, year: int, month: int) -> dict:
    base = Path(base_out_dir)
    month_str = f"{year:04d}-{month:02d}"
    monthly_file = (
        base
        / model
        / f"{year:04d}"
        / f"{month:02d}"
        / f"monthly_merged_digest_{model}_{month_str}.pkl"
    )

    if not monthly_file.exists():
        raise FileNotFoundError(f"Monthly pickle not found: {monthly_file}")

    with open(monthly_file, "rb") as fh:
        data = pickle.load(fh)

    return data


def insert_month_into_db(
    db_path: Path,
    model: str,
    year: int,
    month: int,
    monthly_data: dict,
    compression: int,
):
    if not db_path.exists():
        print(f"Database does not exist: {db_path}")
        return

    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=OFF;")

        cur.execute("BEGIN;")
        try:
            rows = []
            for id_string, payload in monthly_data.items():
                centroids = payload["centroids"]
                quantiles = payload["quantiles"]
                qlist = payload["quantile_list"]

                rows.append(
                    (
                        model,
                        year,
                        month,
                        id_string,
                        compression,
                        pickle.dumps(
                            centroids, protocol=pickle.HIGHEST_PROTOCOL
                        ),
                        pickle.dumps(
                            quantiles, protocol=pickle.HIGHEST_PROTOCOL
                        ),
                        pickle.dumps(
                            qlist, protocol=pickle.HIGHEST_PROTOCOL
                        ),
                    )
                )

            cur.executemany(
                """
                INSERT INTO geosfp (
                    model, year, month, id_string,
                    compression, centroids, quantiles, quantile_list
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(model, year, month, id_string)
                DO UPDATE SET
                    compression   = excluded.compression,
                    centroids     = excluded.centroids,
                    quantiles     = excluded.quantiles,
                    quantile_list = excluded.quantile_list
                """,
                rows,
            )

            conn.commit()
            print(f"Inserted/updated {len(rows)} rows for {model} {year}-{month:02d}")
        except Exception:
            conn.rollback()
            raise


def main():
    base_out_dir = "/home/sadhika8/JupyterLinks/nobackup/quads_data"
    model = "GEOSFP"
    year = 2024
    month = 5
    compression = 300

    db_path = Path(
        "/home/sadhika8/JupyterLinks/nobackup/quads_database/"
        "geosfp_monthly_aggregated_centroids_and_quantiles.db"
    )

    monthly_data = load_monthly_pickle(
        base_out_dir=base_out_dir,
        model=model,
        year=year,
        month=month,
    )

    insert_month_into_db(
        db_path=db_path,
        model=model,
        year=year,
        month=month,
        monthly_data=monthly_data,
        compression=compression,
    )


if __name__ == "__main__":
    main()

