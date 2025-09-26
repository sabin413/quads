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
    Find a list of files in each collections for a model/date using only the collection token embedded in filenames.

    Behavior:
      - Resolves the root directory from a YAML file, also gets a list of 'COLLECTIONS'.
      - For each collection listed in the YAML, finds files whose names contain the
        collection token (substring match) within that directory tree.
      - Keeps only files with .nc or .nc4 extensions.
      - Returns a list of all files, a (collection: files_list) dictionary,
        and the list of excluded variables EXCLUDED_VARS.

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

    # Collections must be provided in the YAML.
    collections = [str(c).strip() for c in cfg['COLLECTIONS'] if str(c).strip()]

    allowed_exts = {'.nc', '.nc4'}
    files: list[str] = []
    collection_map: Dict[str, list[str]] = {}

    # Search per collection by token in filename; filter by extension.
    for c in collections:
        pattern = f"*{c}*"
        hits = [
            str(p)
            for p in root.rglob(pattern)
            if p.suffix in allowed_exts
        ]
        collection_map[c] = hits
        files.extend(hits)

    # Excluded vars.
    excluded = list(cfg.get('EXCLUDED_VARS'))

    return files, collection_map, excluded

if __name__ == "__main__":
    results = list_files_and_excluded_vars("GEOSFP", datetime(2024, 6, 25),"/home/sadhika8/JupyterLinks/nobackup/quads/conf/dataserver.yaml") 
    print(len(results[1]['inst3_2d_asm_Nx']))


## NOTE:1.  The file names for each collection is done simply matching the collection name embedded inside the file names. More robust matching can be done
# by putting more matching criterion, such as matching file patterns using the "FILES" entry (currently not used) from the .YAML
# 2. To include more models, expand the .YAML file

