import sqlite3

DB = "data/imdb.db"

EXPECTED = {
    "movies": {"movie_id", "title_type", "primary_title", "original_title", "is_adult", "start_year", "end_year", "runtime_minutes"},
    "ratings": {"movie_id", "average_rating", "num_votes"},
    "genres": {"movie_id", "genre"},
    "persons": {"person_id", "name", "birth_year", "death_year"},
    "principals": {"movie_id", "person_id", "ordering", "category", "job"},
    "directors": {"movie_id", "person_id"},
    "writers": {"movie_id", "person_id"},
    "characters": {"movie_id", "person_id", "name"},
    "titles": {"movie_id", "region", "title"},
}

def cols(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    return {r[1] for r in cur.fetchall()}

con = sqlite3.connect(DB)
cur = con.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]

print("=== TABLES ===")
for t in tables:
    print("-", t)

print("\n=== CHECK COLS ===")
ok = True
for t, need in EXPECTED.items():
    if t not in tables:
        print(f"[NO TABLE] {t}")
        ok = False
        continue
    have = cols(cur, t)
    missing = need - have
    if missing:
        print(f"[MISSING] {t}: {sorted(missing)}")
        ok = False
    else:
        print(f"[OK] {t}")

print("\n=== SAMPLE JOIN ===")
cur.execute("""
SELECT m.movie_id, m.primary_title, r.average_rating
FROM movies m
LEFT JOIN ratings r ON r.movie_id = m.movie_id
LIMIT 5
""")
for row in cur.fetchall():
    print(row)

print("\nRESULT:", "OK ✅" if ok else "NOT OK ❌")
con.close()
