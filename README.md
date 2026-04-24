# NSE Corporate Actions Tracker — Automated Analyst Alerts

Daily Python pipeline that fetches corporate action announcements from NSE's public API, cleans inconsistent data formats using Pandas, writes structured records to a SQL database, and queries upcoming events across a monitored watchlist to generate automated daily alert reports.

---

## Project Structure

```
nse_actions_tracker/
├── db/
│   ├── schema.sql              # SQL tables: corporate_actions, watchlist, alert_log
│   ├── connection.py           # SQLAlchemy engine
│   └── load_watchlist.py       # Seeds watchlist table from CSV
├── reports/
│   └── alert_report.py         # Queries SQL, flags upcoming events, exports CSV
├── data/
│   └── watchlist.csv           # 20 Nifty stocks to monitor
├── fetcher.py                  # NSE API fetch → Pandas clean → SQL write
├── main.py                     # Orchestrator + daily scheduler
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### 1. Database

```sql
CREATE DATABASE nse_tracker;
```

### 2. Environment

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 3. Install

```bash
pip install -r requirements.txt
```

### 4. Initialise

```bash
python -m db.load_watchlist
```

---

## Usage

**Run once (fetch + alert report):**
```bash
python main.py
```

**Run on daily schedule at 16:00 IST:**
```bash
python main.py --schedule
```

**Alert report only (wider window):**
```bash
python -m reports.alert_report --days 14
```

---

## SQL Tables

| Table | Description |
|---|---|
| `corporate_actions` | All fetched NSE announcements, deduplicated |
| `watchlist` | Monitored symbols with priority flags |
| `alert_log` | Daily alert records keyed by date + symbol |

---

## Data Cleaning

Raw NSE data has several quality issues handled by `fetcher.py`:

- Inconsistent date formats (`dd-MMM-yyyy`, `yyyy-mm-dd`, `dd/mm/yyyy`)
- Mixed-case and verbose action type labels (`"Face Value Split"` → `split`)
- Blank or missing ex-date and symbol fields
- Duplicate announcements for the same event

---

## Alert Flags

Events flagged as **HIGH** priority: `split`, `buyback`, `rights`, `bonus`

These directly affect analyst models — share counts, price adjustments, dilution — and require proactive model updates before the ex-date.

---

## Stack

Python · Pandas · SQLAlchemy · PostgreSQL · psycopg2 · requests · schedule
