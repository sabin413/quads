from pathlib import Path
from datetime import datetime
from typing import Dict
import yaml

def list_files_and_excluded_vars(
    model: str,
    date: datetime,
    data_yaml_file: str | Path
) -> tuple[list[str], Dict[str, list[str]], list[str]]:
    """
    For given model and date, finds a list of files in each collections using only the collection token embedded in filenames.
      - Reads the address of the data directory from an YAML file, also gets a list of 'COLLECTIONS'.
      - For each collection listed in the YAML, finds files whose names contain the
        collection token (substring match) within that directory tree.
      - Keeps only files with .nc or .nc4 extensions.
      - Returns a list of all files, a (collection: files_list) dictionary,
        and the list of excluded variables EXCLUDED_VARS.
      - Please see a sample yaml file (/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml) 

    Parameters
    ----------
    model : str
        which model? (e.g., "GEOSFP").
    date : datetime
        date for which your are analyzing files
    data_yaml_file : str | Path
        Path to the YAML config.

    Returns
    -------
    files : list[str]
         list of all matching file paths.
    collection_map : Dict[str, list[str]]
        Mapping of collection -> list of matching file paths.
    excluded : list[str]
        Variables to exclude from the YAML (EXCLUDED_VARS).
    """
    cfg = yaml.safe_load(Path(data_yaml_file).read_text())['MODELS'][model]

    # Resolve the search root for this date.
    root = Path(date.strftime(cfg['SRC'])).expanduser()
    
    #print(root)

    # Collections must be provided in the YAML.
    collections = [str(c).strip() for c in cfg['COLLECTIONS'] if str(c).strip()]

    allowed_exts = {'.nc', '.nc4'}
    files: list[str] = []
    collection_map: Dict[str, list[str]] = {}

    # Search per collection by token in filename; filter by extension.
    for c in collections:
        #pattern = f"*{c}*"
        if '.' in c:
            pattern = f"*{c}.*"
        else:
            pattern = f"*{c}.[0-9]*"

        hits = [
            str(p)
            for p in root.glob(pattern) # this does not look into subdirs
            if p.suffix in allowed_exts
        ]
        collection_map[c] = hits
        files.extend(hits)

    # Excluded vars.
    excluded = list(cfg.get('EXCLUDED_VARS'))

    return files, collection_map, excluded

if __name__ == "__main__":
    results = list_files_and_excluded_vars("MERRA2", datetime(2024, 6, 25),"/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml") 
    #print(results[1]['inst1_2d_int_Nx'])
    dic = results[1]
    for key, value in dic.items():
        print(key, len(value))
    print(dic["tavgM_3d_trb_Np"])

## NOTE:1.  The file names for each collection is done simply matching the collection name embedded inside the file names. More robust matching can be done
# by putting more matching criterion, such as matching file patterns using the "FILES" entry (currently not used) from the .YAML


