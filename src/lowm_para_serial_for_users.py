# from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Dict
import pickle
import gzip

import yaml
import xarray as xr
import pandas as pd
import dask
from dask import delayed
from dask.diagnostics import Profiler, ResourceProfiler, visualize

from get_collections_and_files import list_files_and_excluded_vars
from make_tdigest import create_digest, get_quantiles_from_tdigest
from tdfence import check_limit
from pytdigest import TDigest  # for typing of `digest`

from strid_uuid import make_id_uuid

# Coordinate name preferences (in order)
LEV_NAMES = ["lev", "level", "pressure"]
LAT_NAMES = ["lat", "latitude", "y"]


# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------
def load_strata(strata_yaml_file: str | Path) -> Dict[str, Dict]:
    """Load the 'STRATA' section from a YAML file."""
    with open(strata_yaml_file, "r") as f:
        return yaml.safe_load(f)["STRATA"]


def save_one_result(key, digest, quantiles, limits, out_dir: Path) -> None:
    fname = out_dir / f"{key}.pkl.gz"
    with gzip.open(fname, "wb") as fh:
        pickle.dump(
            {
                "id_key": key,
                "centroids": digest.get_centroids(),   # <-- keep this
                "quantiles": quantiles,                # 
                "limits": limits,
            },
            fh,
            protocol=pickle.HIGHEST_PROTOCOL,
        )


# will later save the results in a sqlite database

# ------------------------------------------------------------------
# main driver
# ------------------------------------------------------------------
def compute_and_save_results(
    model: str,
    date: datetime,
    data_yaml_file: str,
    strata_file: str,
    out_dir: str,
) -> None:
    """Computes t-digests and saves results."""
    # 1) Resolve files and collections from YAML.
    _, collection_dict, excluded = list_files_and_excluded_vars(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
    )

    # 2) Load strata definitions.
    strata = load_strata(strata_file)

    # 3) Ensure output directories exist.
    OUT_DIR = Path(out_dir)
    OUT_DIR.mkdir(exist_ok=True)

    DATE_DIR = OUT_DIR / date.strftime("%Y-%m-%d")
    DATE_DIR.mkdir(parents=True, exist_ok=True)

    final_results = []
    #id_rows = []
    # 4) For each collection, open datasets and build jobs.
    for coll_name, files in list(collection_dict.items()):
        if coll_name != "inst3_2d_asm_Nx":
            break
        # 4.1) Open multi-file dataset lazily with xarray/dask.
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
        print(ds.dims)
        end = datetime.now()
        print(f"time to open metadataset: {(end - start).total_seconds()}")

        # 4.2) Identify coordinate names (lat, optional level).
        lat_name = next(c for c in LAT_NAMES if c in ds.coords)
        lev_dim = next((d for d in LEV_NAMES if d in ds.coords), None)

        delayed_jobs = []
        stored_results = []

        # 4.3) For each variable...
        for var in ds.data_vars:
            da = ds[var]
            #print(var)
            # 4.4) For each level (or None if no level coord)...
            levels = (
                ds[lev_dim].values
                if lev_dim and (lev_dim in da.coords)
                else [None]
            )

            for lev_val in levels:
                da_lev = da.sel({lev_dim: lev_val}) if lev_val is not None else da

                # 4.5) For each stratum (lat band)...
                for sname, sdef in strata.items(): # strata name, defination
                    #print("sname", "sdef", sname, sdef)
                    lat_slice = slice(sdef["lat"]["min"], sdef["lat"]["max"])
                    #print(lat_slice)# gives slice(20, 45, None/deault step)
                    dims2stack = [d for d in da_lev.dims] 
                    #print(dims2stack) # gives [lon, lat, time]
                    # 4.6) Build a flat sample array (lazy dask array).
                    flat = (
                        da_lev.sel({lat_name: lat_slice})
                        .stack(sample=dims2stack)
                        .data.reshape(-1)  # lazy
                    )

                    #print(flat.shape) # multiple time dimensions from multiple files

                    id_key = f"{coll_name}|{var}|{lev_val}|{sname}"
                    # find idstring and uuid here
                    
                    #stratum_da = da_lev.sel({lat_name: lat_slice})
                    #idstring, uid = make_id_uuid(stratum_da, model, coll_name, var, lat_name, lev_dim, nlev_total=(len(ds[lev_dim]) if lev_dim else 0))
                    #print(idstring, uid)
                    #id_rows.append({"model": model, "collection": coll_name,"variable": var,"level": -1 if lev_val is None 
                                   # else lev_val,"stratum": sname,"idstring": idstring,"uuid": uid,})


                    # 4.7) Define delayed analytics pipeline for this slice.
                    @delayed
                    def analyse(arr, key=id_key, month, year, model):
                        #digest = create_digest(arr, compression=300)
                        #quantiles = get_quantiles_from_tdigest(digest)
                        #limits = check_limit(arr)
                        #save_one_result(key, digest, quantiles, limits, DATE_DIR) 
                        #stored_results.append([key, digest, quantiles, limits])
                        #return key  # small confirmation
                       
                        # identify the database address(model)
                        # go to relevant table query(id_key, month, year)
                        # get percentile - query_percentile_from_db
                        # now, perform sanity check

                    # 4.8) Queue the job.
                    delayed_jobs.append(analyse(flat))
                    #print(f"var: {var}, level: {lev_val}, strata: {sname}, total chunks analyzed: {len(delayed_jobs)}")

        # 5) Execute all delayed jobs (reads ➜ computes ➜ saves ➜ frees RAM).
        timeline_html = f"dask_timeline_{coll_name}.html"
        with Profiler() as prof, ResourceProfiler(dt=0.1) as rprof:
            start = datetime.now()
            finished = dask.compute(*delayed_jobs, scheduler="threads")
            end = datetime.now()
            print(
                f"✔ {len(finished)} results written for collection {coll_name} "
                f"in {(end - start).total_seconds()} secs"
            )

        # 6) (Optional) Save Dask timeline chart.
        # visualize([prof, rprof], filename=timeline_html)
        # print(f"Dask timeline saved: {timeline_html}")

        # 7) Accumulate results for this collection.
        final_results.extend(stored_results)

    # 8) Return results across all collections.
    #ids_df = pd.DataFrame(id_rows, columns=["model","collection","variable","level","stratum","idstring","uuid"])
    # save alongside results
    #ids_df.to_csv(DATE_DIR / "ids.csv", index=False)

    return final_results


# ------------------------------------------------------------------
if __name__ == "__main__":
    out_dir = "/home/sadhika8/JupyterLinks/nobackup/quads_v1_results/geosfp"
    model = "GEOSFP"
    date = datetime(2024, 6, 19)
    data_yaml_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml"
    strata_file = "/home/sadhika8/JupyterLinks/nobackup/quads/conf/strata.yaml"

    results = compute_and_save_results(
        model=model,
        date=date,
        data_yaml_file=data_yaml_file,
        strata_file=strata_file,
        out_dir=out_dir,
    )

