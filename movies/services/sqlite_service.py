# movies/services/sqlite_service.py
from __future__ import annotations

import math
import sqlite3
from typing import Any, Dict, List, Optional

from django.conf import settings


def _sqlite_path() -> str:
    """
    Compat: certains projets utilisent SQLITE_PATH, d'autres IMDB_SQLITE_PATH.
    """
    path = getattr(settings, "IMDB_SQLITE_PATH", None) or getattr(settings, "SQLITE_PATH", None)
    if not path:
        raise RuntimeError("SQLite path not configured. Set IMDB_SQLITE_PATH (or SQLITE_PATH) in settings.py")
    return str(path)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_path())
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------
#  Helpers "counts" (compat)
# ---------------------------
def list_tables() -> List[str]:
    with _connect() as conn:
        rows = conn.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """).fetchall()
    return [r["name"] for r in rows]


def table_count(table: str) -> int:
    # table vient de sqlite_master => safe à quotter en "..."
    with _connect() as conn:
        row = conn.execute(f'SELECT COUNT(*) AS n FROM "{table}"').fetchone()
    return int(row["n"])


def all_table_counts(limit: int = 50) -> Dict[str, int]:
    """
    Utilisé par views.py chez toi. Donne un dict {table: nb_lignes}.
    """
    tables = list_tables()[:limit]
    return {t: table_count(t) for t in tables}


# ---------------------------
#  Data access (pages)
# ---------------------------
def list_genres(limit: int = 200) -> List[str]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT genre FROM genres WHERE genre IS NOT NULL ORDER BY genre LIMIT ?",
            (limit,),
        ).fetchall()
    return [r["genre"] for r in rows]


def list_movies(
    page: int = 1,
    per_page: int = 20,
    genre: str = "all",
    year_min: Optional[int] = None,
    year_max: Optional[int] = None,
    rating_min: Optional[float] = None,
    sort: str = "title_asc",
) -> Dict[str, Any]:
    """
    Schéma IMDB classique :
      movies(movie_id, title_type, primary_title, start_year, runtime_minutes, ...)
      ratings(movie_id, average_rating, num_votes)
      genres(movie_id, genre)
    """
    page = max(1, int(page))
    per_page = max(1, min(int(per_page), 200))

    where = ["m.title_type = 'movie'"]
    params: List[Any] = []

    if genre and genre != "all":
        where.append("EXISTS (SELECT 1 FROM genres g WHERE g.movie_id = m.movie_id AND g.genre = ?)")
        params.append(genre)

    if year_min not in (None, ""):
        where.append("m.start_year >= ?")
        params.append(int(year_min))

    if year_max not in (None, ""):
        where.append("m.start_year <= ?")
        params.append(int(year_max))

    if rating_min not in (None, ""):
        where.append("r.average_rating >= ?")
        params.append(float(rating_min))

    where_sql = " AND ".join(where)

    sort_map = {
        "title_asc": "m.primary_title ASC",
        "title_desc": "m.primary_title DESC",
        "year_asc": "m.start_year ASC",
        "year_desc": "m.start_year DESC",
        "rating_asc": "COALESCE(r.average_rating, -1) ASC",
        "rating_desc": "COALESCE(r.average_rating, -1) DESC",
    }
    order_by = sort_map.get(sort, sort_map["title_asc"])

    offset = (page - 1) * per_page

    sql_count = f"""
        SELECT COUNT(*) AS n
        FROM movies m
        LEFT JOIN ratings r ON r.movie_id = m.movie_id
        WHERE {where_sql}
    """

    sql_page = f"""
        SELECT
            m.movie_id            AS movie_id,
            m.primary_title       AS title,
            m.start_year          AS year,
            m.runtime_minutes     AS runtime,
            r.average_rating      AS average_rating,
            r.num_votes           AS num_votes
        FROM movies m
        LEFT JOIN ratings r ON r.movie_id = m.movie_id
        WHERE {where_sql}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
    """

    with _connect() as conn:
        total = conn.execute(sql_count, params).fetchone()["n"]
        rows = conn.execute(sql_page, (*params, per_page, offset)).fetchall()

    nb_pages = max(1, math.ceil(total / per_page))
    movies = [dict(r) for r in rows]

    return {
        "movies": movies,
        "total": total,
        "page": page,
        "per_page": per_page,
        "nb_pages": nb_pages,
        "filters": {
            "genre": genre,
            "year_min": year_min,
            "year_max": year_max,
            "rating_min": rating_min,
            "sort": sort,
        },
        "genres": list_genres(),
    }


def search_all(q: str, limit_movies: int = 10, limit_people: int = 10) -> Dict[str, Any]:
    q = (q or "").strip()
    if not q:
        return {"q": q, "movies": [], "people": []}

    like = f"%{q}%"

    sql_movies = """
        SELECT m.movie_id AS movie_id, m.primary_title AS title, m.start_year AS year
        FROM movies m
        WHERE m.title_type='movie' AND m.primary_title LIKE ?
        ORDER BY COALESCE(m.start_year, 0) DESC
        LIMIT ?
    """

    sql_people = """
        SELECT p.person_id AS person_id, p.name AS name
        FROM persons p
        WHERE p.name LIKE ?
        ORDER BY p.name ASC
        LIMIT ?
    """

    with _connect() as conn:
        movies = [dict(r) for r in conn.execute(sql_movies, (like, int(limit_movies))).fetchall()]
        people = [dict(r) for r in conn.execute(sql_people, (like, int(limit_people))).fetchall()]

    return {"q": q, "movies": movies, "people": people}


def stats() -> Dict[str, Any]:
    with _connect() as conn:
        n_movies = conn.execute("SELECT COUNT(*) AS n FROM movies WHERE title_type='movie'").fetchone()["n"]
        n_people = conn.execute("SELECT COUNT(*) AS n FROM persons").fetchone()["n"]

        by_genre = conn.execute("""
            SELECT genre, COUNT(*) AS n
            FROM genres
            GROUP BY genre
            ORDER BY n DESC
            LIMIT 20
        """).fetchall()

        by_decade = conn.execute("""
            SELECT (m.start_year/10)*10 AS decade, COUNT(*) AS n
            FROM movies m
            WHERE m.title_type='movie' AND m.start_year IS NOT NULL
            GROUP BY decade
            ORDER BY decade ASC
        """).fetchall()

        ratings_hist = conn.execute("""
            SELECT CAST(r.average_rating AS INT) AS bin, COUNT(*) AS n
            FROM ratings r
            GROUP BY bin
            ORDER BY bin ASC
        """).fetchall()

        top_actors = conn.execute("""
            SELECT p.name AS name, COUNT(DISTINCT pr.movie_id) AS n
            FROM principals pr
            JOIN persons p ON p.person_id = pr.person_id
            JOIN movies m ON m.movie_id = pr.movie_id
            WHERE m.title_type='movie' AND pr.category IN ('actor','actress')
            GROUP BY p.person_id
            ORDER BY n DESC
            LIMIT 10
        """).fetchall()

    return {
        "n_movies": n_movies,
        "n_people": n_people,
        "by_genre": [dict(r) for r in by_genre],
        "by_decade": [dict(r) for r in by_decade],
        "ratings_hist": [dict(r) for r in ratings_hist],
        "top_actors": [dict(r) for r in top_actors],
    }
