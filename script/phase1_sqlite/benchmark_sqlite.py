# script/phase1_sqlite/benchmark_sqlite.py

from __future__ import annotations

import os
import time
import sqlite3
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Any

from queries import (
    query_actor_filmography,
    query_top_n_movies,
    query_multi_role_actors,
    query_collaborations,
    query_popular_genres,
    query_career_evolution,
    query_top3_by_genre,
    query_career_boost,
    query_most_versatile_actors,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
DB_PATH = ROOT_DIR / "data" / "imdb.db"

# --------------------------------------------------------------------
# Indexes qu’on va tester pour T1.4
# --------------------------------------------------------------------
INDEXES = [
    ("idx_persons_name", "CREATE INDEX IF NOT EXISTS idx_persons_name ON persons(name);"),
    ("idx_principals_person", "CREATE INDEX IF NOT EXISTS idx_principals_person ON principals(person_id);"),
    ("idx_principals_movie", "CREATE INDEX IF NOT EXISTS idx_principals_movie ON principals(movie_id);"),
    ("idx_genres_genre", "CREATE INDEX IF NOT EXISTS idx_genres_genre ON genres(genre);"),
    ("idx_movies_start_year", "CREATE INDEX IF NOT EXISTS idx_movies_start_year ON movies(start_year);"),
]


# --------------------------------------------------------------------
# Requêtes à benchmarker (T1.3)
# --------------------------------------------------------------------
def make_queries() -> Dict[str, Callable[[sqlite3.Connection], List[Tuple[Any, ...]]]]:
    return {
        # Q1 : filmographie d’un acteur
        "Q1 - Filmographie (Tom Hanks)": lambda conn: query_actor_filmography(conn, "Tom Hanks"),

        # Q2 : Top N films d’un genre sur une période
        "Q2 - Top 50 Drama 1990-2020": lambda conn: query_top_n_movies(
            conn, genre="Drama", start_year=1990, end_year=2020, n=50
        ),

        # Q3 : Acteurs multi-rôles
        "Q3 - Acteurs multi-rôles": lambda conn: query_multi_role_actors(conn),

        # Q4 : Collaborations avec acteur donné
        "Q4 - Collaborations (Tom Hanks)": lambda conn: query_collaborations(conn, "Tom Hanks"),

        # Q5 : Genres populaires
        "Q5 - Genres populaires": lambda conn: query_popular_genres(conn),

        # Q6 : Évolution de carrière
        "Q6 - Carrière (Tom Hanks)": lambda conn: query_career_evolution(conn, "Tom Hanks"),

        # Q7 : Top 3 par genre
        "Q7 - Top 3 par genre": lambda conn: query_top3_by_genre(conn),

        # Q8 : Carrières boostées
        "Q8 - Carrières boostées": lambda conn: query_career_boost(conn),

        # Q9 : Acteurs polyvalents
        "Q9 - Acteurs polyvalents": lambda conn: query_most_versatile_actors(conn, min_genres=3, limit=50),
    }


# --------------------------------------------------------------------
# Utils
# --------------------------------------------------------------------
def time_query(conn: sqlite3.Connection, func: Callable[[sqlite3.Connection], Any], repeats: int = 3) -> float:
    """
    Retourne le temps moyen en millisecondes pour exécuter la requête `repeats` fois.
    """
    # 1 exécution de chauffe
    _ = func(conn)

    t0 = time.perf_counter()
    for _ in range(repeats):
        rows = func(conn)
        # on force la matérialisation
        _ = len(rows)
    t1 = time.perf_counter()

    return (t1 - t0) * 1000.0 / repeats  # ms


def print_table(results: List[Tuple[str, float, float]]):
    """
    results: liste (label, t_no_idx_ms, t_with_idx_ms)
    """
    print("\n" + "=" * 80)
    print("Benchmark SQLite (T1.4)")
    print("=" * 80)
    headers = ["Requête", "Sans index (ms)", "Avec index (ms)", "Gain (%)"]

    # calcul largeur colonnes
    rows_fmt = []
    for label, t1, t2 in results:
        if t1 > 0:
            gain = 100.0 * (t1 - t2) / t1
        else:
            gain = 0.0
        rows_fmt.append((label, f"{t1:.2f}", f"{t2:.2f}", f"{gain:.1f}"))

    widths = [len(h) for h in headers]
    for row in rows_fmt:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))

    def fmt_row(row):
        return " | ".join(str(val).ljust(widths[i]) for i, val in enumerate(row))

    sep = "-+-".join("-" * w for w in widths)

    print(fmt_row(headers))
    print(sep)
    for row in rows_fmt:
        print(fmt_row(row))


# --------------------------------------------------------------------
# EXPLAIN QUERY PLAN pour illustrer dans le rapport
# --------------------------------------------------------------------
def explain_example(conn: sqlite3.Connection):
    """
    Exemple d'utilisation d'EXPLAIN QUERY PLAN pour Q1 et Q2.
    Tu peux copier/coller la sortie dans le rapport.
    """

    cursor = conn.cursor()

    # Q1 : filmographie
    sql_q1 = """
    SELECT
        m.primary_title,
        m.start_year,
        c.name AS character,
        r.average_rating
    FROM movies      AS m
    JOIN principals  AS p  ON p.movie_id  = m.movie_id
    JOIN persons     AS pe ON pe.person_id = p.person_id
    LEFT JOIN characters AS c
           ON c.movie_id  = m.movie_id
          AND c.person_id = p.person_id
    LEFT JOIN ratings AS r
           ON r.movie_id = m.movie_id
    WHERE pe.name LIKE ?
      AND p.category IN ('actor', 'actress')
    ORDER BY m.start_year DESC, m.primary_title ASC;
    """

    print("\n=== EXPLAIN QUERY PLAN Q1 (filmographie) ===")
    for row in cursor.execute("EXPLAIN QUERY PLAN " + sql_q1, ("%Tom Hanks%",)):
        print(row)

    # Q2 : top N drama
    sql_q2 = """
    SELECT
        m.primary_title,
        m.start_year,
        r.average_rating,
        r.num_votes
    FROM movies  AS m
    JOIN genres  AS g ON g.movie_id = m.movie_id
    JOIN ratings AS r ON r.movie_id = m.movie_id
    WHERE g.genre = ?
      AND m.start_year BETWEEN ? AND ?
    ORDER BY r.average_rating DESC,
             r.num_votes DESC,
             m.primary_title ASC
    LIMIT ?;
    """

    print("\n=== EXPLAIN QUERY PLAN Q2 (Top N Drama) ===")
    for row in cursor.execute("EXPLAIN QUERY PLAN " + sql_q2, ("Drama", 1990, 2020, 50)):
        print(row)


# --------------------------------------------------------------------
# Main T1.4
# --------------------------------------------------------------------
def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")

    # Taille avant index
    size_before = os.path.getsize(DB_PATH)

    # On supprime d'abord les index qu'on contrôle, pour avoir le cas “sans index”
    cur = conn.cursor()
    for name, _ in INDEXES:
        cur.execute(f"DROP INDEX IF EXISTS {name};")
    conn.commit()

    queries = make_queries()

    # ---- temps sans index ----
    times_no_idx = {}
    print("Mesure des temps SANS index supplémentaires...")
    for label, func in queries.items():
        t = time_query(conn, func, repeats=3)
        times_no_idx[label] = t
        print(f"{label}: {t:.2f} ms (sans index)")

    # ---- création des index ----
    print("\nCréation des index...")
    for _, ddl in INDEXES:
        cur.execute(ddl)
    conn.commit()

    # Taille après index
    size_after = os.path.getsize(DB_PATH)

    # ---- temps avec index ----
    times_with_idx = {}
    print("\nMesure des temps AVEC index...")
    for label, func in queries.items():
        t = time_query(conn, func, repeats=3)
        times_with_idx[label] = t
        print(f"{label}: {t:.2f} ms (avec index)")

    conn.close()

    # ---- tableau de synthèse ----
    results = []
    for label in queries.keys():
        t1 = times_no_idx[label]
        t2 = times_with_idx[label]
        results.append((label, t1, t2))

    print_table(results)

    print("\nTaille de la base :")
    print(f"- Avant index : {size_before / (1024*1024):.2f} Mo")
    print(f"- Après index : {size_after / (1024*1024):.2f} Mo")

    # Exemple d'EXPLAIN pour le rapport
    conn2 = sqlite3.connect(DB_PATH)
    explain_example(conn2)
    conn2.close()


if __name__ == "__main__":
    main()
