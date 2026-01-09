# movies/services/sqlite_service.py
import sqlite3
from django.conf import settings


def get_sqlite_connection():
    conn = sqlite3.connect(settings.IMDB_SQLITE_PATH)
    conn.row_factory = sqlite3.Row  # IMPORTANT: dict-like rows
    return conn


def _fetchone_dict(cur):
    row = cur.fetchone()
    return dict(row) if row else None


def _fetchall_dict(cur):
    return [dict(r) for r in cur.fetchall()]


def get_movie_by_id(movie_id: str):
    """
    Retourne un dict complet:
    movie_id, title_type, primary_title, original_title, start_year, runtime_minutes,
    average_rating, num_votes, genres(list), directors(list), writers(list), cast(list), alt_titles(list)
    """
    with get_sqlite_connection() as conn:
        cur = conn.cursor()

        # 1) Base movie + rating
        cur.execute(
            """
            SELECT
                m.movie_id,
                m.title_type,
                m.primary_title,
                m.original_title,
                m.is_adult,
                m.start_year,
                m.end_year,
                m.runtime_minutes,
                r.average_rating,
                r.num_votes
            FROM movies m
            LEFT JOIN ratings r ON r.movie_id = m.movie_id
            WHERE m.movie_id = ?
            """,
            (movie_id,),
        )
        movie = _fetchone_dict(cur)
        if not movie:
            return None

        # 2) Genres
        cur.execute(
            "SELECT genre FROM genres WHERE movie_id = ? ORDER BY genre",
            (movie_id,),
        )
        movie["genres"] = [r["genre"] for r in cur.fetchall()]

        # 3) Directors
        cur.execute(
            """
            SELECT p.person_id, p.name
            FROM directors d
            JOIN persons p ON p.person_id = d.person_id
            WHERE d.movie_id = ?
            ORDER BY p.name
            """,
            (movie_id,),
        )
        movie["directors"] = _fetchall_dict(cur)

        # 4) Writers
        cur.execute(
            """
            SELECT p.person_id, p.name
            FROM writers w
            JOIN persons p ON p.person_id = w.person_id
            WHERE w.movie_id = ?
            ORDER BY p.name
            """,
            (movie_id,),
        )
        movie["writers"] = _fetchall_dict(cur)

        # 5) Cast / principals (+ character name si dispo)
        cur.execute(
            """
            SELECT
                pr.ordering,
                pr.person_id,
                p.name,
                pr.category,
                pr.job,
                c.name AS character_name
            FROM principals pr
            JOIN persons p ON p.person_id = pr.person_id
            LEFT JOIN characters c
                ON c.movie_id = pr.movie_id AND c.person_id = pr.person_id
            WHERE pr.movie_id = ?
            ORDER BY pr.ordering
            """,
            (movie_id,),
        )
        movie["cast"] = _fetchall_dict(cur)

        # 6) Alternative titles (optionnel)
        cur.execute(
            """
            SELECT region, title
            FROM titles
            WHERE movie_id = ?
            ORDER BY region
            LIMIT 50
            """,
            (movie_id,),
        )
        movie["alt_titles"] = _fetchall_dict(cur)

        return movie


def search_movies(q: str, limit: int = 50):
    q = (q or "").strip()
    if not q:
        return []

    like = f"%{q}%"
    with get_sqlite_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                m.movie_id,
                m.primary_title,
                m.start_year,
                r.average_rating,
                r.num_votes
            FROM movies m
            LEFT JOIN ratings r ON r.movie_id = m.movie_id
            WHERE m.primary_title LIKE ? OR m.original_title LIKE ?
            ORDER BY COALESCE(r.num_votes, 0) DESC
            LIMIT ?
            """,
            (like, like, limit),
        )
        return _fetchall_dict(cur)


def search_people(q: str, limit: int = 50):
    q = (q or "").strip()
    if not q:
        return []
    like = f"%{q}%"

    with get_sqlite_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT person_id, name, birth_year, death_year
            FROM persons
            WHERE name LIKE ?
            ORDER BY name
            LIMIT ?
            """,
            (like, limit),
        )
        return _fetchall_dict(cur)
