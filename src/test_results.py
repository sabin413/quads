import pandas as pd

df = pd.read_pickle("quads_test_2025_05_19.pkl")

print(df.shape)       # (n_rows, n_cols)
print(df.columns)     # column names
print(df.head())      # first few rows

