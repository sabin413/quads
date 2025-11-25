# lowm_para_serial_for_users.py
from pathlib import Path
from datetime import datetime
from typing import Dict
import pickle
import sqlite3
import yaml
import xarray as xr
import pandas as pd
import dask
from dask import delayed

from get_collections_and_files import list_files_and_excluded_vars
from sanity_check_v2 import edge_check

# Coordinate name preferences (in order)
LEV_NAMES = ["lev", "level", "pressure"]
LAT_NAMES = ["lat", "latitude", "y"]


# -----------------------------
# helpers
# -----------------------------
def load_strata(strata_yaml_file: str | Path) -> Dict[str, Dict]:
    """Load the 'STRATA' section from a YAML file."""
    with open(strata_yaml_file, "r") as f:
        return yaml.safe_load(f)["STRATA"]


def load_quantiles_from_db(db_path: Path, model: str, year: int, month: int, id_string: str):
    """
    Fetch (quantiles, quantile_list) for a given key/month from SQLite.
    HARD FAIL if not found.
    """
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT quantiles, quantile_list FROM geosfp "
            "WHERE model=? AND year=? AND month=? AND id_string=?",
            (model, year, month, id_string),
        ).fetchone()

    if not row:
        raise RuntimeError(
            f"No reference quantiles in DB for "
            f"(model={model}, year={year}, month={month}, id_string='{id_string}') "
            f"at {db_path}"
        )

    return pickle.loads(row[0]), pickle.loads(row[1])


# -----------------------------
# main driver
# -----------------------------
def compute_and_save_results(
    model: str,
    date: datetime,
    data_yaml_file: str,
    strata_file: str,
    historical_reference_date: datetime,
    db_path: str | Path,
) -> pd.DataFrame:
    """
    For each (collection,var,level,stratum) slice:
      - flatten data
      - fetch reference quantiles from SQLite (for this model/year/month/id)
      - run sanity check (edge_check)
      - return row: id_string, no_of_violations, quantiles, quantile_list
    """
    # Resolve files and exclusions from YAML
    _, collection_dict, excluded = list_files_and_excluded_vars(
        model=model, date=date, data_yaml_file=data_yaml_file
    )

    # Load strata definitions
    strata = load_strata(strata_file)

    delayed_jobs = []

    for coll_name, files in list(collection_dict.items()):
        #if coll_name != "inst3_2d_asm_Nx":
        #    break

        ds = xr.open_mfdataset(
            files,
            combine="by_coords",
            drop_variables=excluded,
            data_vars="minimal",
            coords="minimal",
            compat="no_conflicts",
            engine="h5netcdf",
            chunks="auto",
            parallel=True,
        )
        print(ds.dims)

        lat_name = next(c for c in LAT_NAMES if c in ds.coords)
        lev_dim = next((d for d in LEV_NAMES if d in ds.coords), None)

        for var in ds.data_vars:
            da = ds[var]
            levels = ds[lev_dim].values if (lev_dim and (lev_dim in da.coords)) else [None]

            for lev_val in levels:
                da_lev = da.sel({lev_dim: lev_val}) if lev_val is not None else da

                for sname, sdef in strata.items():
                    lat_slice = slice(sdef["lat"]["min"], sdef["lat"]["max"])
                    dims2stack = [d for d in da_lev.dims]

                    flat = (
                        da_lev.sel({lat_name: lat_slice})
                        .stack(sample=dims2stack)
                        .data.reshape(-1)
                    )

                    id_key = f"{coll_name}|{var}|{lev_val}|{sname}"

                    @delayed
                    def analyse(
                        arr,
                        key=id_key,
                        _model=model,
                        _y=historical_reference_date.year,
                        _m=historical_reference_date.month,
                        _db=Path(db_path),
                    ):
                        data = arr
                        quantiles, qlist = load_quantiles_from_db(_db, _model, _y, _m, key)
                        n_low, n_high = edge_check(data, (quantiles, qlist))
                        no_viol = int(n_low + n_high)
                        return key, no_viol, quantiles, qlist

                    delayed_jobs.append(analyse(flat))
    print(len(delayed_jobs))
    finished = dask.compute(*delayed_jobs, scheduler="threads")

    df = pd.DataFrame(
        finished,
        columns=["id_string", "no_of_violations", "quantile_values", "q_list"],
    )
    return df


# -----------------------------
# __main__
# -----------------------------
if __name__ == "__main__":
    model = "GEOSFP"
    date = datetime(2025, 5, 10)             
    historical_reference_date = datetime(2024, 5, 1) # should actually look at exactly 1 year ago, and month only. day index is dummy here  
    data_yaml_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml"
    strata_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml"
    db_path = Path(
        f"/home/sadhika8/JupyterLinks/nobackup/quads_database/{model}_monthly_aggregated_centroids_and_quantiles.db"
    )

    df = compute_and_save_results(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
        strata_file=strata_file,
        historical_reference_date=historical_reference_date,
        db_path=db_path,
    )

    df_var_name = f"quads_test_{date.strftime('%Y_%m_%d')}"
    globals()[df_var_name] = df

    out_file = f"{df_var_name}.pkl"
    df.to_pickle(out_file)

    print(f"Created DataFrame '{df_var_name}' with {len(df)} rows.")
    print(f"✔ Saved DataFrame to ./{out_file}")
    print(df.head())

