"""
reports/alert_report.py
------------------------
Queries the SQL database to surface upcoming corporate actions
across the watchlist, flags high-priority events (splits, buybacks,
rights, bonus), and generates a structured daily alert report as CSV.

Usage:
    python -m reports.alert_report
    python -m reports.alert_report --days 14
"""

import argparse
import os
from datetime import date

import pandas as pd
from sqlalchemy import text

from db.connection import engine

OUTPUT_DIR    = "reports"
HIGH_PRIORITY = {"split", "buyback", "rights", "bonus"}
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# QUERY
# ─────────────────────────────────────────────

def fetch_upcoming_alerts(today: date, horizon_days: int = 7) -> pd.DataFrame:
    """
    Query SQL: JOIN corporate_actions against watchlist,
    filter for events with ex_date within the next horizon_days,
    return sorted by ex_date ascending.
    """
    query = text("""
        SELECT
            ca.symbol,
            ca.company_name,
            ca.action_type,
            ca.ex_date,
            ca.record_date,
            ca.details,
            w.priority,
            (ca.ex_date - :today) AS days_until
        FROM
            corporate_actions ca
        JOIN
            watchlist w ON ca.symbol = w.symbol
        WHERE
            ca.ex_date BETWEEN :today AND :horizon
        ORDER BY
            ca.ex_date ASC,
            w.priority DESC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            "today"  : str(today),
            "horizon": str(today + pd.Timedelta(days=horizon_days)),
        })
    return df


def fetch_recent_activity(lookback_days: int = 30) -> pd.DataFrame:
    """
    Query SQL: count corporate actions per symbol over the last N days.
    Flags symbols with unusually high activity (>= 3 events).
    """
    query = text("""
        SELECT
            ca.symbol,
            COUNT(*) AS action_count,
            STRING_AGG(DISTINCT ca.action_type, ', ') AS action_types
        FROM
            corporate_actions ca
        JOIN
            watchlist w ON ca.symbol = w.symbol
        WHERE
            ca.fetched_at >= NOW() - INTERVAL ':days days'
        GROUP BY
            ca.symbol
        HAVING
            COUNT(*) >= 3
        ORDER BY
            action_count DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"days": lookback_days})


# ─────────────────────────────────────────────
# WRITE ALERTS TO SQL
# ─────────────────────────────────────────────

def write_alerts_to_sql(df: pd.DataFrame, today: date):
    if df.empty:
        return
    records = df[["symbol", "action_type", "ex_date", "days_until", "priority"]].copy()
    records["alert_date"] = today
    records.to_sql("alert_log", engine, if_exists="append", index=False, method="multi")
    print(f"  {len(records):,} alerts written to SQL alert_log.")


# ─────────────────────────────────────────────
# PRINT + EXPORT
# ─────────────────────────────────────────────

def export_report(df: pd.DataFrame, today: date, horizon_days: int):
    if df.empty:
        print("  No upcoming events in watchlist.")
        return

    # Flag high-priority
    df["flag"] = df["action_type"].apply(
        lambda x: "HIGH" if x in HIGH_PRIORITY else "normal"
    )

    # Console summary
    print(f"\n  Upcoming corporate actions — next {horizon_days} days")
    print(f"  {'Symbol':<12} {'Action':<10} {'Ex-Date':<12} {'Days':<6} {'Priority':<8} {'Flag'}")
    print("  " + "-" * 62)
    for _, row in df.iterrows():
        print(f"  {row['symbol']:<12} {row['action_type']:<10} {str(row['ex_date']):<12} "
              f"{int(row['days_until']):<6} {row.get('priority','normal'):<8} {row['flag']}")

    # CSV export
    csv_path = os.path.join(OUTPUT_DIR, f"alerts_{today}.csv")
    df.to_csv(csv_path, index=False)
    print(f"\n  CSV saved → {csv_path}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(horizon_days: int = 7):
    today = date.today()
    print(f"\n{'='*50}")
    print(f"  ALERT REPORT  |  {today}  |  horizon: {horizon_days}d")
    print(f"{'='*50}\n")

    df = fetch_upcoming_alerts(today, horizon_days)
    print(f"  {len(df):,} upcoming events found in watchlist.")

    write_alerts_to_sql(df, today)
    export_report(df, today, horizon_days)
    print("\nAlert report complete.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7,
                        help="Lookahead window in days (default: 7)")
    args = parser.parse_args()
    run(args.days)
