"""
db/connection.py
----------------
Centralised SQLAlchemy engine. All modules import from here.
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost:5432/nse_tracker"
)

engine       = create_engine(DB_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        sql = f.read()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print("  Database initialised.")
