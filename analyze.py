import pickle, gzip, pathlib, pprint

# 1) pick the first *.pkl or *.pkl.gz in results/
pkl_path = next(pathlib.Path("results").rglob("*.pkl*"))

print(f"Opening: {pkl_path}")

# 2) open with gzip if it’s compressed, else plain open
opener = gzip.open if pkl_path.suffix == ".gz" else open
with opener(pkl_path, "rb") as fh:
    rec = pickle.load(fh)

# 3) pretty-print the dictionary
pprint.pprint(rec, depth=2, compact=True)

