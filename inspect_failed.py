# inspect_failed.py
import pandas as pd

df = pd.read_csv("data/transactions.csv", parse_dates=["timestamp"])
print("TOTAL ROWS:", len(df))
failed = df[df["status"] != "success"]
print("FAILED ROWS:", len(failed))
print("\n-- Failed sample (up to 20 rows) --")
if not failed.empty:
    print(failed.head(20).to_string(index=False))
else:
    print("No failed rows found.")
print("\n-- Top buckets by failed count --")
# bucket by 5-min (string slice is fine)
buckets = df.copy()
buckets['bucket'] = buckets['timestamp'].astype(str).str.slice(0,16)  # up to minutes
counts = buckets.groupby('bucket')['status'].apply(lambda s: (s!="success").sum()).sort_values(ascending=False)
print(counts.head(20).to_string())
