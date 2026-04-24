"""
main.py
-------
Orchestrates the full daily pipeline:
  1. Fetch new corporate actions from NSE → clean → write to SQL
  2. Query SQL → generate alert report for watchlist

Run once:
    python main.py

Run on schedule (daily at 16:00 IST, after market close):
    python main.py --schedule
"""

import argparse
import schedule
import time
from datetime import datetime

from fetcher import run as fetch_and_store
from reports.alert_report import run as generate_alerts


def run_pipeline():
    print(f"\n{'#'*50}")
    print(f"  PIPELINE START  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'#'*50}")
    fetch_and_store()
    generate_alerts(horizon_days=7)
    print(f"{'#'*50}")
    print(f"  PIPELINE COMPLETE")
    print(f"{'#'*50}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule", action="store_true",
                        help="Run daily at 16:00 instead of once")
    parser.add_argument("--days", type=int, default=7,
                        help="Alert horizon in days")
    args = parser.parse_args()

    if args.schedule:
        print("Scheduler active — running daily at 16:00 IST.")
        schedule.every().day.at("16:00").do(run_pipeline)
        run_pipeline()   # run immediately on start
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_pipeline()
