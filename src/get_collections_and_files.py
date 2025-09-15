from pathlib import Path
from datetime import datetime
import yaml
import xarray as xr
import dask


from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple
# works for geosfp for now, but can be easily extended to work for all models


from make_tdigest import create_digest
from make_tdigest import get_quantiles_from_tdigest
from tdfence import check_limit


def list_files_and_excluded_fields(
    model: str,
    date: datetime,
    yaml_file: str | Path
) -> tuple[list[str], list[str]]:
    """
    Return
    -------
    files     : list[str]   – every data-file path that matches model/date rules
    excluded  : list[str]   – variables whose entry is “…: off” (read once,
                              assuming all collections share the same list)
    """
    cfg = yaml.safe_load(Path(yaml_file).read_text())['MODELS'][model]

    # 1. directory + base filename mask (still contains %C)
    root = Path(date.strftime(cfg['SRC'])).expanduser()
    #mask = cfg['FILES']
    # mask = string1.*string2.nc 
    #print(root)
    #print(mask)
    #print(mask.replace('%C', collections[0])) 
    # 2. which collections?  explicit keys or “all” (*)
    coll_block  = cfg.get('COLLECTIONS', {})
    #print(coll_block)
    collections = [k for k in coll_block if k != '*'] or ['*']
    #print(collections)
    #print(mask.replace('*', collections[0]))
    # 3. gather files for each collection  ← only this block is changed
    #ymd = date.strftime('%Y%m%d')                     # 20240618
    files = []
    collection_dict = {}
    for coll in collections:
        coll = coll.strip()
        pattern = f"*{coll}*"
        #print(str(root.rglob(pattern)))
        files.extend(str(p) for p in root.rglob(pattern))
        collection_dict[coll] = [str(p) for p in root.rglob(pattern)]
    files.sort()
    # 4. find files to be excluded
    excluded_fields = [list(element.keys())[0] for element in cfg["COLLECTIONS"]['*']]
    return files, collection_dict, excluded_fields


def load_strata(yaml_file: str | Path) -> Dict[str, Dict]:
    with open(yaml_file, "r") as f:
        return yaml.safe_load(f)["STRATA"]


#files1 = coll_dict["inst3_3d_chm_Nv"]


#files, vars_to_drop = files1, demo_excluded
#CHUNKS = {"time": 1, "lat": 256, "lon": 256, "lev": 1}
#print("start")

strata      = load_strata("conf/strata.yaml")       # {stratum_name: {"lat":{min,max}, …}}
LEV_NAMES   = {"lev", "level", "pressure"}
LAT_NAMES   = {"lat", "latitude", "y"}

# where results will be written
#out_dir = Path("results") ; out_dir.mkdir(exist_ok=True)

def compute_and_save_results(model='GEOSFP',
        date=datetime(2024, 6, 18),          # pick the date / cycle you need
        yaml_file='conf/dataserver.yaml'):
    ''' process all the data and compute tdigest for a given model and day'''

    _, collection_dict, excluded_vars = list_files_and_excluded_fields(model=model,date=date, yaml_file=yaml_file)

    for key, value in collection_dict.items():
        files = value # all files in a collection having same set of vars
        ds = xr.open_mfdataset(
                files,
                # Handles “some files have lev, some don’t”; aligns by coordinate values
                combine="by_coords",
                # Throw away variables we’re not interested in before graph is built
                drop_variables=excluded_vars,
                data_vars="minimal",      # only concat vars that *depend* on concat dim
                coords="minimal",
                compat="no_conflicts",    # ignore harmless attribute mismatches
                engine="h5netcdf",
                chunks='auto',
                parallel=True)

        ds = ds.persist()        # pin chunks for reuse across many slices

        # Identify coordinate names just once
        lat_name = next(c for c in LAT_NAMES if c in ds.coords)
        lev_dim  = next((d for d in LEV_NAMES if d in ds.dims), None)

        # Build containers for the lazy slices + IDs
        flat_slices, id_keys = [], []

        # --------------------------------------------------------------
        # 1-b  Enumerate   variable  ×  level  ×  stratum
        # --------------------------------------------------------------
        for var in ds.data_vars:
            da = ds[var]

        # levels to iterate over (None if the data has no vertical dim)
        levels = ds[lev_dim].values if (lev_dim and lev_dim in da.dims) else [None]

        for lev_val in levels:
            # slice by level (skip if None)
            da_lev = da.sel({lev_dim: lev_val}, method="nearest") if lev_val is not None else da

            for stratum_name, sdef in strata.items():
                lat_slice = slice(sdef["lat"]["min"], sdef["lat"]["max"])

                # build lazy 1-D view  (time kept as first dim)
                dims_to_stack = [d for d in da_lev.dims if d != "time"]
                flat = (da_lev
                        .sel({lat_name: lat_slice})
                        .stack(sample=dims_to_stack)
                        .data)                # Dask array, still lazy

                flat_slices.append(flat)
                id_keys.append(f"{coll_name}|{var}|{lev_val}|{stratum_name}")

        # --------------------------------------------------------------
        # 1-c  Materialise in one (or batched) compute
        # --------------------------------------------------------------
        numpy_arrays = dask.compute(*flat_slices, scheduler="threads")

    # --------------------------------------------------------------
    # 1-d  Apply analysis functions & save
    # --------------------------------------------------------------
    for key, arr in zip(id_keys, numpy_arrays):
        digest  = tdigest(arr)
        limits  = limit_check(arr)

        # save as pickle  (you can switch to YAML/JSON if you prefer)
        with open(out_dir / f"{key}.pkl", "wb") as f:
            pickle.dump({"tdigest": digest, "limits": limits}, f)



