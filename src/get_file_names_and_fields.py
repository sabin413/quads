from pathlib import Path
from datetime import datetime
import yaml
import xarray as xr
# works for geosfp for now, but can be easily extended to work for all models

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
    print(root)
    #print(mask)
    #print(mask.replace('%C', collections[0])) 
    # 2. which collections?  explicit keys or “all” (*)
    coll_block  = cfg.get('COLLECTIONS', {})
    #print(coll_block)
    collections = [k for k in coll_block if k != '*'] or ['*']
    print(collections)
    #print(mask.replace('*', collections[0]))
    # 3. gather files for each collection  ← only this block is changed
    #ymd = date.strftime('%Y%m%d')                     # 20240618
    files = []
    for coll in collections:
        coll = coll.strip()
        pattern = f"*{coll}*"
        #print(str(root.rglob(pattern)))
        files.extend(str(p) for p in root.rglob(pattern))
    files.sort()
    # 4. find files to be excluded
    excluded_fields = [list(element.keys())[0] for element in cfg["COLLECTIONS"]['*']]
    return files, excluded_fields


# -------------------------------------------------------------------------
# demo / sanity-check
# -------------------------------------------------------------------------
if __name__ == '__main__':
    demo_files, demo_excluded = list_files_and_excluded_fields(
        model='GEOSFP',
        date=datetime(2024, 6, 18),          # pick the date / cycle you need
        yaml_file='conf/dataserver.yaml'
    )

    print(f'FILES ({len(demo_files)}):')
    for f in demo_files:
        print('  ', f)

    print('\nEXCLUDED FIELDS:')
    print(', '.join(demo_excluded) or '  (none)')
    
    for file in demo_files:
        with xr.open_dataset(file) as ds:
            print(f"{file}: {', '.join(ds.coords)}")
