import sqlite3
from django.conf import settings

def get_connection():
    return sqlite3.connect(settings.IMDB_SQLITE_PATH)

def list_tables():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        return [r[0] for r in cur.fetchall()]

def table_count(table_name: str) -> int:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return int(cur.fetchone()[0])

def all_table_counts(limit: int = 20) -> dict:
    tables = list_tables()[:limit]
    return {t: table_count(t) for t in tables}
