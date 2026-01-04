# script/phase1_sqlite/show_queries.py

from pathlib import Path
import sqlite3

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

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "imdb.db"


def print_section(title: str, headers, rows, limit: int = 10):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

    rows = list(rows)[:limit]
    if not rows:
        print("(aucun résultat)")
        return

    # largeur colonnes
    cols = len(headers)
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i in range(cols):
            widths[i] = max(widths[i], len(str(row[i])))

    # ligne header
    header_line = " | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    sep_line = "-+-".join("-" * widths[i] for i in range(cols))
    print(header_line)
    print(sep_line)

    # lignes données
    for row in rows:
        line = " | ".join(str(row[i]).ljust(widths[i]) for i in range(cols))
        print(line)


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")

    # 1. Filmographie d’un acteur
    rows = query_actor_filmography(conn, "Tom Hanks")
    print_section(
        "Filmographie de Tom Hanks (exemple T1.3)",
        ["Titre", "Année", "Personnage", "Note"],
        rows,
        limit=15,
    )

    # 2. Top N films d’un genre
    rows = query_top_n_movies(conn, genre="Drama", start_year=1990, end_year=2020, n=10)
    print_section(
        "Top 10 films 'Drama' (1990–2020)",
        ["Titre", "Année", "Note", "Votes"],
        rows,
    )

    # 3. Acteurs avec plusieurs rôles dans un même film
    rows = query_multi_role_actors(conn)
    print_section(
        "Acteurs avec plusieurs personnages dans un même film",
        ["Acteur", "Film", "Année", "Nb rôles"],
        rows,
    )

    # 4. Réalisateurs qui collaborent avec un acteur
    rows = query_collaborations(conn, "Tom Hanks")
    print_section(
        "Réalisateurs ayant le plus collaboré avec Tom Hanks",
        ["Réalisateur", "Nb films ensemble"],
        rows,
    )

    # 5. Genres populaires
    rows = query_popular_genres(conn)
    print_section(
        "Genres populaires (note moyenne > 7 et > 50 films)",
        ["Genre", "Nb films", "Note moyenne"],
        rows,
    )

    # 6. Évolution de carrière
    rows = query_career_evolution(conn, "Tom Hanks")
    print_section(
        "Évolution de la carrière de Tom Hanks par décennie",
        ["Décennie", "Nb films", "Note moyenne"],
        rows,
    )

    # 7. Top 3 films par genre
    rows = query_top3_by_genre(conn)
    print_section(
        "Top 3 films par genre",
        ["Genre", "Rang", "Titre", "Année", "Note"],
        rows,
        limit=30,
    )

    # 8. Carrières « boostées »
    rows = query_career_boost(conn)
    print_section(
        "Carrières boostées par un film à gros succès",
        ["Personne", "Nb films low", "Nb films high", "Année percée"],
        rows,
    )

    # 9. Acteurs les plus polyvalents
    rows = query_most_versatile_actors(conn, min_genres=3, limit=20)
    print_section(
        "Acteurs les plus polyvalents (au moins 3 genres)",
        ["Acteur", "Nb genres", "Nb films"],
        rows,
    )

    conn.close()


if __name__ == "__main__":
    main()
