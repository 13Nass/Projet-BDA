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

def conn():
    c = sqlite3.connect(str(settings.SQLITE_PATH))
    c.row_factory = sqlite3.Row
    return c

def list_movies(page:int, page_size:int, genre=None, year_min=None, year_max=None,
               rating_min=None, sort="title_asc"):
    offset = (page - 1) * page_size
    where = []
    params = []

    base = """
    FROM movies m
    LEFT JOIN ratings r ON r.movie_id = m.movie_id
    """

    if genre:
        base += " JOIN genres g ON g.movie_id = m.movie_id "
        where.append("g.genre = ?")
        params.append(genre)

    if year_min:
        where.append("m.year >= ?"); params.append(int(year_min))
    if year_max:
        where.append("m.year <= ?"); params.append(int(year_max))
    if rating_min:
        where.append("r.average_rating >= ?"); params.append(float(rating_min))

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sort_map = {
        "title_asc": "m.title ASC",
        "title_desc": "m.title DESC",
        "year_asc": "m.year ASC",
        "year_desc": "m.year DESC",
        "rating_asc": "r.average_rating ASC",
        "rating_desc": "r.average_rating DESC",
    }
    order_sql = sort_map.get(sort, "m.title ASC")

    c = conn()
    total = c.execute(f"SELECT COUNT(DISTINCT m.movie_id) AS c {base} {where_sql}", params).fetchone()["c"]

    q = f"""
    SELECT DISTINCT m.movie_id, m.title, m.year, r.average_rating, r.num_votes
    {base}
    {where_sql}
    ORDER BY {order_sql}
    LIMIT ? OFFSET ?
    """
    rows = c.execute(q, params + [page_size, offset]).fetchall()
    c.close()
    return total, [dict(r) for r in rows]

def list_genres():
    c = conn()
    rows = c.execute("SELECT DISTINCT genre FROM genres ORDER BY genre").fetchall()
    c.close()
    return [r["genre"] for r in rows]

def search_all(q: str, limit_movies=30, limit_people=30):
    qlike = f"%{q.strip()}%"
    c = conn()
    movies = c.execute("""
        SELECT m.movie_id, m.title, m.year
        FROM movies m
        WHERE m.title LIKE ?
        ORDER BY m.year DESC
        LIMIT ?
    """, (qlike, limit_movies)).fetchall()

    people = c.execute("""
        SELECT p.person_id, p.name
        FROM persons p
        WHERE p.name LIKE ?
        ORDER BY p.name ASC
        LIMIT ?
    """, (qlike, limit_people)).fetchall()

    c.close()
    return [dict(r) for r in movies], [dict(r) for r in people]

def stats_data():
    c = conn()
    # films par genre
    by_genre = c.execute("""
        SELECT g.genre, COUNT(DISTINCT g.movie_id) AS n
        FROM genres g
        GROUP BY g.genre
        ORDER BY n DESC
        LIMIT 20
    """).fetchall()

    # films par décennie
    by_decade = c.execute("""
        SELECT (m.year/10)*10 AS decade, COUNT(*) AS n
        FROM movies m
        WHERE m.year IS NOT NULL
        GROUP BY decade
        ORDER BY decade
    """).fetchall()

    # distribution des notes (bins 0.5)
    ratings_hist = c.execute("""
        SELECT CAST(average_rating*2 AS INT)/2.0 AS bin, COUNT(*) AS n
        FROM ratings
        WHERE average_rating IS NOT NULL
        GROUP BY bin
        ORDER BY bin
    """).fetchall()

    # top 10 acteurs prolifiques (par nb films)
    top_actors = c.execute("""
        SELECT p.person_id, p.name, COUNT(DISTINCT pr.movie_id) AS n
        FROM principals pr
        JOIN persons p ON p.person_id = pr.person_id
        WHERE pr.category IN ('actor','actress')
        GROUP BY p.person_id
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()

    c.close()
    return {
        "by_genre": [dict(r) for r in by_genre],
        "by_decade": [dict(r) for r in by_decade],
        "ratings_hist": [dict(r) for r in ratings_hist],
        "top_actors": [dict(r) for r in top_actors],
    }
