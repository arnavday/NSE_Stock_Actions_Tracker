"""
db/load_watchlist.py
---------------------
Reads watchlist.csv and inserts symbols into the watchlist SQL table.

Usage:
    python -m db.load_watchlist
"""

import os
import pandas as pd
from db.connection import engine, init_db

WATCHLIST_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "watchlist.csv")


def load():
    init_db()
    df = pd.read_csv(WATCHLIST_CSV)
    df.to_sql("watchlist", engine, if_exists="replace", index=False, method="multi")
    print(f"  {len(df)} symbols loaded into watchlist.")


if __name__ == "__main__":
    load()
