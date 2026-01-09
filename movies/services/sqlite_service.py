"""
SQLite service layer for CineExplorer / IMDB dataset.

This module intentionally returns plain Python dicts / tuples so templates can
render without relying on Django ORM models.

Expected schema (underscore columns):
- movies(movie_id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes)
- ratings(movie_id, average_rating, num_votes)
- genres(movie_id, genre)
- persons(person_id, name, birth_year, death_year)
- directors(movie_id, person_id)
- writers(movie_id, person_id)
- principals(movie_id, person_id, ordering, category, job)
- titles(movie_id, region, title)

If your DB differs, adapt the SQL below.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from django.conf import settings


# ---------- connection helpers ----------

def _get_db_path() -> str:
    # Prefer explicit settings.IMDB_SQLITE_PATH (you already have it in settings.py)
    db_path = getattr(settings, "IMDB_SQLITE_PATH", None)
    if db_path:
        return str(db_path)

    # Fallbacks (project root)
    base_dir = Path(getattr(settings, "BASE_DIR", Path.cwd()))
    for candidate in (base_dir / "data" / "imdb.db", base_dir / "imdb.db", base_dir / "db.sqlite3"):
        if candidate.exists():
            return str(candidate)

    # Last resort
    return str(base_dir / "db.sqlite3")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _fetchone(sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def _fetchall(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with _connect() as conn:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def _fetchcol(sql: str, params: Sequence[Any] = ()) -> List[Any]:
    with _connect() as conn:
        cur = conn.execute(sql, params)
        return [r[0] for r in cur.fetchall()]


def _normalize_region(region: Optional[str]) -> str:
    if not region:
        return ""
    r = str(region).strip()
    if r in ("\\N", "None"):
        return ""
    if r.lower() in ("nan", "null"):
        return ""
    return r


# ---------- public API used by views/templates ----------

def list_top_movies(limit: int = 12) -> List[Dict[str, Any]]:
    """
    Returns list of movies sorted by rating then votes.
    Each movie is a dict and contains at least: id, movie_id, primary_title, start_year, rating, votes, genres
    """
    sql = """
        SELECT
            m.movie_id,
            m.primary_title,
            m.start_year,
            r.average_rating AS rating,
            r.average_rating AS average_rating,
            r.num_votes AS votes,
            r.num_votes AS num_votes,
            GROUP_CONCAT(DISTINCT g.genre) AS genres_csv
        FROM movies m
        LEFT JOIN ratings r ON r.movie_id = m.movie_id
        LEFT JOIN genres g ON g.movie_id = m.movie_id
        WHERE m.title_type = 'movie'
        GROUP BY m.movie_id
        ORDER BY
            (r.average_rating IS NULL) ASC,
            r.average_rating DESC,
            (r.num_votes IS NULL) ASC,
            r.num_votes DESC
        LIMIT ?
    """
    rows = _fetchall(sql, (limit,))
    for row in rows:
        row["id"] = row.get("movie_id")  # convenience for templates
        genres_csv = row.pop("genres_csv", None)
        if genres_csv:
            genres = [g.strip() for g in str(genres_csv).split(",") if g.strip()]
            row["genres"] = ", ".join(genres)
        else:
            row["genres"] = ""
    return rows


def list_recent_movies(limit: int = 12) -> List[Dict[str, Any]]:
    """
    Returns recent movies by start_year DESC.
    """
    sql = """
        SELECT
            m.movie_id,
            m.primary_title,
            m.start_year,
            r.average_rating AS rating,
            r.average_rating AS average_rating,
            r.num_votes AS votes,
            r.num_votes AS num_votes,
            GROUP_CONCAT(DISTINCT g.genre) AS genres_csv
        FROM movies m
        LEFT JOIN ratings r ON r.movie_id = m.movie_id
        LEFT JOIN genres g ON g.movie_id = m.movie_id
        WHERE m.title_type = 'movie' AND m.start_year IS NOT NULL
        GROUP BY m.movie_id
        ORDER BY m.start_year DESC, (r.num_votes IS NULL) ASC, r.num_votes DESC
        LIMIT ?
    """
    rows = _fetchall(sql, (limit,))
    for row in rows:
        row["id"] = row.get("movie_id")
        genres_csv = row.pop("genres_csv", None)
        if genres_csv:
            genres = [g.strip() for g in str(genres_csv).split(",") if g.strip()]
            row["genres"] = ", ".join(genres)
        else:
            row["genres"] = ""
    return rows


def get_movie_by_id(movie_id: str) -> Optional[Dict[str, Any]]:
    """
    Returns a full movie dict for the detail page.
    """
    base_sql = """
        SELECT
            m.movie_id,
            m.title_type,
            m.primary_title,
            m.original_title,
            m.is_adult,
            m.start_year,
            m.end_year,
            m.runtime_minutes,
            r.average_rating AS rating,
            r.average_rating AS average_rating,
            r.num_votes AS votes,
            r.num_votes AS num_votes,
            GROUP_CONCAT(DISTINCT g.genre) AS genres_csv
        FROM movies m
        LEFT JOIN ratings r ON r.movie_id = m.movie_id
        LEFT JOIN genres g ON g.movie_id = m.movie_id
        WHERE m.movie_id = ?
        GROUP BY m.movie_id
        LIMIT 1
    """
    movie = _fetchone(base_sql, (movie_id,))
    if not movie:
        return None

    movie["id"] = movie.get("movie_id")  # template convenience

    genres_csv = movie.pop("genres_csv", None)
    if genres_csv:
        genres = [g.strip() for g in str(genres_csv).split(",") if g.strip()]
        movie["genres"] = ", ".join(genres)
    else:
        movie["genres"] = ""

    # Directors
    dir_sql = """
        SELECT p.name
        FROM directors d
        JOIN persons p ON p.person_id = d.person_id
        WHERE d.movie_id = ?
        ORDER BY p.name
    """
    movie["directors"] = _fetchcol(dir_sql, (movie_id,))

    # Writers
    wri_sql = """
        SELECT p.name
        FROM writers w
        JOIN persons p ON p.person_id = w.person_id
        WHERE w.movie_id = ?
        ORDER BY p.name
    """
    movie["writers"] = _fetchcol(wri_sql, (movie_id,))

    # Cast (actors/actresses)
    cast_sql = """
        SELECT p.name, pr.category
        FROM principals pr
        JOIN persons p ON p.person_id = pr.person_id
        WHERE pr.movie_id = ? AND pr.category IN ('actor', 'actress')
        ORDER BY pr.ordering ASC
        LIMIT 20
    """
    cast_rows = _fetchall(cast_sql, (movie_id,))
    movie["cast"] = [f"{r['name']} ({r['category']})" for r in cast_rows if r.get("name")]

    # Alternate titles
    alt_sql = """
        SELECT t.title, t.region
        FROM titles t
        WHERE t.movie_id = ?
        ORDER BY t.region IS NULL, t.region, t.title
        LIMIT 25
    """
    alt_rows = _fetchall(alt_sql, (movie_id,))
    alts: List[str] = []
    for r in alt_rows:
        title = (r.get("title") or "").strip()
        if not title:
            continue
        region = _normalize_region(r.get("region"))
        alts.append(f"{title} [{region}]" if region else title)
    movie["alt_titles"] = alts

    # If some templates still expect pk, provide it (no downside)
    movie["pk"] = movie.get("movie_id")

    return movie


def search_movies(query: str, limit: int = 30) -> List[Dict[str, Any]]:
    q = (query or "").strip()
    if not q:
        return []
    like = f"%{q}%"
    sql = """
        SELECT
            m.movie_id,
            m.primary_title,
            m.start_year,
            r.average_rating AS rating,
            r.average_rating AS average_rating,
            r.num_votes AS votes,
            r.num_votes AS num_votes
        FROM movies m
        LEFT JOIN ratings r ON r.movie_id = m.movie_id
        WHERE m.title_type = 'movie'
          AND (m.primary_title LIKE ? OR m.original_title LIKE ?)
        ORDER BY
            (r.average_rating IS NULL) ASC,
            r.average_rating DESC,
            (r.num_votes IS NULL) ASC,
            r.num_votes DESC
        LIMIT ?
    """
    rows = _fetchall(sql, (like, like, limit))
    for row in rows:
        row["id"] = row.get("movie_id")
    return rows


def search_people(query: str, limit: int = 30) -> List[Dict[str, Any]]:
    q = (query or "").strip()
    if not q:
        return []
    like = f"%{q}%"
    sql = """
        SELECT person_id, name, birth_year, death_year
        FROM persons
        WHERE name LIKE ?
        ORDER BY name
        LIMIT ?
    """
    return _fetchall(sql, (like, limit))


def search_all(query: str, limit_movies: int = 20, limit_people: int = 20) -> Dict[str, Any]:
    return {
        "movies": search_movies(query, limit_movies),
        "people": search_people(query, limit_people),
    }


def list_genres(limit: int = 25) -> List[Tuple[str, int]]:
    """
    Used by stats.html: expects (genre, count) pairs.
    """
    sql = """
        SELECT genre, COUNT(*) AS cnt
        FROM genres
        WHERE genre IS NOT NULL AND TRIM(genre) <> ''
        GROUP BY genre
        ORDER BY cnt DESC, genre ASC
        LIMIT ?
    """
    with _connect() as conn:
        cur = conn.execute(sql, (limit,))
        rows = cur.fetchall()
        return [(str(r["genre"]), int(r["cnt"])) for r in rows]


def all_table_counts() -> List[Tuple[str, int]]:
    """
    Returns (table_name, count) for all user tables.
    """
    with _connect() as conn:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()]
        out: List[Tuple[str, int]] = []
        for name in tables:
            # Safety: allow only [a-zA-Z0-9_]
            if not all(c.isalnum() or c == "_" for c in name):
                continue
            cnt = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            out.append((name, int(cnt)))
        return out


# Backward-compatible aliases (in case views import these names)
get_top_movies = list_top_movies
get_recent_movies = list_recent_movies
