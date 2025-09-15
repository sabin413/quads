from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
import yaml
import xarray as xr
import dask
from dask.diagnostics import Profiler, ResourceProfiler, visualize
from dask import delayed
# ------------------------------------------------------------------

#from pytdigest import TDigest
from get_collections_and_files import list_files_and_excluded_fields
from make_tdigest import create_digest
from make_tdigest import get_quantiles_from_tdigest
from tdfence import check_limit

# ------------------------------------------------------------------ helpers --
def load_strata(yaml_file: str | Path) -> Dict[str, Dict]:
    with open(yaml_file, "r") as f:
        return yaml.safe_load(f)["STRATA"]


# ── configurable bits ─────────────────────────────────────────────
LEV_NAMES = {"lev", "level", "pressure"}
LAT_NAMES = {"lat", "latitude", "y"}
OUT_DIR   = Path("results"); OUT_DIR.mkdir(exist_ok=True)

def save_one_result(key: str, digest, quantiles, limits) -> None:
    """Persist a single result to disk (runs inside a worker)."""
    import pickle, gzip
    fname = OUT_DIR / f"{key}.pkl.gz"
    with gzip.open(fname, "wb") as fh:
        pickle.dump(
            dict(
                id_key    = key,
                centroids = digest.get_centroids(),
                quantiles = quantiles,
                limits    = limits,
            ),
            fh,
        )

# ------------------------------------------------------------------
# main driver
# ------------------------------------------------------------------
def compute_and_save_results(
    model: str = "GEOSFP",
    date:  datetime = datetime(2024, 6, 18),
    yaml_file: str = "conf/dataserver.yaml",
) -> None:
    ''' does this and that '''
    # collection_dict: for each collection (key), get a list of files(values)  
    _, collection_dict, excluded = list_files_and_excluded_fields(model=model, date=date, yaml_file=yaml_file)
    
    strata = load_strata("conf/strata.yaml")
    # open all files from each collection  parallelly 
    final_results = []
    for coll_name, files in list(collection_dict.items()):
        start = datetime.now() 
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
        end = datetime.now()
        print(f"time to open metadataset: {(end-start).total_seconds()}")
        # discover coordinate names - lat and lev, potentially
        lat_name = next(c for c in LAT_NAMES if c in ds.coords)
        lev_dim  = next((d for d in LEV_NAMES if d in ds.coords), None)

        delayed_jobs = []
        stored_results = []
        # build one delayed job per  var × lev × stratum
        for var in ds.data_vars:
            da = ds[var]

            levels = ( ds[lev_dim].values if lev_dim and (lev_dim in da.coords) else [None] )

            for lev_val in levels:
                da_lev = (
                    da.sel({lev_dim: lev_val})
                    if lev_val is not None
                    else da
                )

                for sname, sdef in strata.items():
                    lat_slice = slice(sdef["lat"]["min"], sdef["lat"]["max"])
                    dims2stack = [d for d in da_lev.dims if d != "time"]

                    flat = (
                        da_lev
                        .sel({lat_name: lat_slice})
                        .stack(sample=dims2stack)
                        .data          # dask array (lazy) - does not trigger computation
                    )

                    id_key = f"{coll_name}|{var}|{lev_val}|{sname}"

                    # ---------- delayed analytics pipeline -------------
                    @delayed
                    def analyse(arr, key=id_key):
                        digest    = create_digest(arr, compression=300)
                        quantiles = get_quantiles_from_tdigest(digest)
                        limits    = check_limit(arr)
                        save_one_result(key, digest, quantiles, limits) # - test it
                        stored_results.append([key, digest, quantiles, limits])
                        return key      # small confirmation
                        
                    delayed_jobs.append(analyse(flat))
                    #print(f"total chunks analyzed: {len(delayed_jobs)}")

        # reads chunks ➜ computes ➜ saves ➜ releases RAM
        # Profile and visualize task parallelism
        timeline_html = f"dask_timeline_{coll_name}.html"
        with Profiler() as prof, ResourceProfiler(dt=0.1) as rprof:
            # kick off all jobs at once; each worker:
            #   reads chunks ➜ computes ➜ saves ➜ releases RAM
            start = datetime.now()
            finished = dask.compute(*delayed_jobs, scheduler="threads")
            end = datetime.now()
            print(f"✔ {len(finished)} results written for collection {coll_name} in {(end-start).total_seconds()} secs")

        # Save timeline chart
        visualize([prof, rprof], filename=timeline_html)
        print(f"Dask timeline saved: {timeline_html}")
    final_results.extend(stored_results)
    return final_results
# ------------------------------------------------------------------
if __name__ == "__main__":
    results = compute_and_save_results()
    print(results)

