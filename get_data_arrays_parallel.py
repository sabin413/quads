# dask_version_of_get_data_arrays.py
import re, yaml, xarray as xr
import numpy as np
from datetime import datetime
from pathlib import Path
from dask import delayed, compute
import time

from get_file_names_and_fields import list_files_and_excluded_fields
# from make_tdigest import create_digest   # ← keep commented if not needed

LAT_NAMES = ("lat", "latitude")
LEV_NAMES = ("lev", "level")


# ----------------------------------------------------------------------
def _slice_file(fp: str, excl: set, strata: dict) -> list[dict]:
    """Same slicing logic as the original but executed inside a Dask task."""
    out, date_tag = [], re.compile(r"\d{8}_\d{4}")
    fname = Path(fp).name
    parts = fname.split(".")
    collection = next(parts[i - 1] for i, p in enumerate(parts)
                      if date_tag.fullmatch(p))

    # modest chunking avoids loading the whole file at once
    with xr.open_dataset(fp, chunks={"lat": 128, "lon": 256}) as ds:
        lat = next(c for c in LAT_NAMES if c in ds.coords)
        for name, da in ds.data_vars.items():
            if name in excl:
                continue
            lev_dim = next((c for c in LEV_NAMES if c in da.dims), None)
            levels = enumerate(da[lev_dim].values) if lev_dim else [(None, None)]
            for idx, lvl_val in levels:
                src = da if lev_dim is None else da.isel({lev_dim: idx})
                for s, r in strata.items():
                    slab = src.sel({lat: slice(r["lat"]["min"], r["lat"]["max"])})
                    flat = slab.values.ravel()                 # NumPy array
                    out.append(
                        dict(
                            file_name=fname,
                            collection=collection,
                            field=name,
                            level=None if lvl_val is None else float(lvl_val),
                            stratum=s,
                            data_array=flat,
                            # digest=create_digest(flat),
                        )
                    )
    return out
# ----------------------------------------------------------------------


def get_data_arrays(model: str, date: datetime) -> list[dict]:
    """Public helper with identical output to the original Pool version."""
    files, excluded = list_files_and_excluded_fields(
        model=model, date=date, yaml_file="conf/dataserver.yaml"
    )
    strata = yaml.safe_load(open("conf/strata.yaml"))["STRATA"]

    # build one delayed task per file
    tasks = [
        delayed(_slice_file)(fp, set(excluded), strata)
        for fp in files
    ]

    # execute in parallel on all local cores
    results_nested = compute(*tasks, scheduler="threads")

    # flatten nested list to match legacy return structure
    return [d for batch in results_nested for d in batch]
# ----------------------------------------------------------------------

if __name__ == "__main__":
    start = time.time()
    data_arrays = get_data_arrays("GEOSFP", datetime(2024, 6, 18))
    print("data_arrays:", len(data_arrays), "| elapsed:", round(time.time() - start, 2), "s")

