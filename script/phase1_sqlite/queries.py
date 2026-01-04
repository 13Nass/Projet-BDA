# scripts/phase1_sqlite/queries.py

from __future__ import annotations

import sqlite3
from typing import List, Tuple, Any


def query_actor_filmography(
    conn: sqlite3.Connection,
    actor_name: str,
) -> List[Tuple[Any, ...]]:
    """
    Retourne la filmographie d’un acteur.

    Args:
        conn: Connexion SQLite ouverte sur imdb.db.
        actor_name: Nom (ou partie de nom) de l’acteur, ex. "Tom Hanks".

    Returns:
        Liste de tuples (titre, année, personnage, note_moyenne) triés par année décroissante.

    SQL utilisé (schéma du projet) :
        SELECT m.primary_title,
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
    sql = """
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
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


def query_top_n_movies(
    conn: sqlite3.Connection,
    genre: str,
    start_year: int,
    end_year: int,
    n: int,
) -> List[Tuple[Any, ...]]:
    """
    Top N films d’un genre donné sur une période, selon la note moyenne.

    Args:
        conn: Connexion SQLite.
        genre: Genre visé (ex. "Drama").
        start_year: Année de début (incluse).
        end_year: Année de fin (incluse).
        n: Nombre de films à retourner.

    Returns:
        Liste de tuples (titre, année, note, nb_votes).

    SQL utilisé :
        SELECT m.primary_title,
               m.start_year,
               r.average_rating,
               r.num_votes
        FROM movies  AS m
        JOIN genres  AS g ON g.movie_id = m.movie_id
        JOIN ratings AS r ON r.movie_id = m.movie_id
        WHERE g.genre = ?
          AND m.start_year BETWEEN ? AND ?
        ORDER BY r.average_rating DESC, r.num_votes DESC, m.primary_title ASC
        LIMIT ?;
    """
    sql = """
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
    return conn.execute(sql, (genre, start_year, end_year, n)).fetchall()


def query_multi_role_actors(conn: sqlite3.Connection) -> List[Tuple[Any, ...]]:
    """
    Acteurs ayant joué plusieurs personnages dans un même film.

    Returns:
        Liste de tuples (nom_acteur, titre_film, année, nb_personnages),
        triés par nb_personnages décroissant.

    SQL utilisé :
        SELECT pe.name,
               m.primary_title,
               m.start_year,
               COUNT(DISTINCT c.name) AS nb_roles
        FROM characters AS c
        JOIN persons    AS pe ON pe.person_id = c.person_id
        JOIN movies     AS m  ON m.movie_id   = c.movie_id
        GROUP BY c.person_id, c.movie_id
        HAVING COUNT(DISTINCT c.name) > 1
        ORDER BY nb_roles DESC, pe.name ASC;
    """
    sql = """
    SELECT
        pe.name,
        m.primary_title,
        m.start_year,
        COUNT(DISTINCT c.name) AS nb_roles
    FROM characters AS c
    JOIN persons    AS pe ON pe.person_id = c.person_id
    JOIN movies     AS m  ON m.movie_id   = c.movie_id
    GROUP BY c.person_id, c.movie_id
    HAVING COUNT(DISTINCT c.name) > 1
    ORDER BY nb_roles DESC, pe.name ASC;
    """
    return conn.execute(sql).fetchall()


def query_collaborations(
    conn: sqlite3.Connection,
    actor_name: str,
) -> List[Tuple[Any, ...]]:
    """
    Réalisateurs ayant collaboré avec un acteur donné, avec le nombre de films ensemble.

    Args:
        conn: Connexion SQLite.
        actor_name: Nom (ou partie de nom) de l’acteur.

    Returns:
        Liste de tuples (nom_réalisateur, nb_films_ensemble), triés par nb_films_ensemble décroissant.

    SQL utilisé (avec sous-requête / CTE) :
        WITH actor_movies AS (
            SELECT DISTINCT m.movie_id
            FROM movies     AS m
            JOIN principals AS p  ON p.movie_id  = m.movie_id
            JOIN persons    AS pe ON pe.person_id = p.person_id
            WHERE pe.name LIKE ?
              AND p.category IN ('actor', 'actress')
        )
        SELECT dpe.name AS director_name,
               COUNT(*)  AS nb_films
        FROM actor_movies AS am
        JOIN directors    AS d   ON d.movie_id   = am.movie_id
        JOIN persons      AS dpe ON dpe.person_id = d.person_id
        GROUP BY dpe.person_id
        ORDER BY nb_films DESC, director_name ASC;
    """
    sql = """
    WITH actor_movies AS (
        SELECT DISTINCT m.movie_id
        FROM movies     AS m
        JOIN principals AS p  ON p.movie_id  = m.movie_id
        JOIN persons    AS pe ON pe.person_id = p.person_id
        WHERE pe.name LIKE ?
          AND p.category IN ('actor', 'actress')
    )
    SELECT
        dpe.name AS director_name,
        COUNT(*) AS nb_films
    FROM actor_movies AS am
    JOIN directors    AS d   ON d.movie_id   = am.movie_id
    JOIN persons      AS dpe ON dpe.person_id = d.person_id
    GROUP BY dpe.person_id
    ORDER BY nb_films DESC, director_name ASC;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


def query_popular_genres(conn: sqlite3.Connection) -> List[Tuple[Any, ...]]:
    """
    Genres populaires : genres ayant une note moyenne > 7.0 et plus de 50 films.

    Returns:
        Liste de tuples (genre, nb_films, note_moyenne) triés par note_moyenne décroissante.

    SQL utilisé :
        SELECT g.genre,
               COUNT(*)              AS nb_films,
               AVG(r.average_rating) AS avg_rating
        FROM genres  AS g
        JOIN ratings AS r ON r.movie_id = g.movie_id
        GROUP BY g.genre
        HAVING AVG(r.average_rating) > 7.0
           AND COUNT(*) > 50
        ORDER BY avg_rating DESC;
    """
    sql = """
    SELECT
        g.genre,
        COUNT(*)              AS nb_films,
        AVG(r.average_rating) AS avg_rating
    FROM genres  AS g
    JOIN ratings AS r ON r.movie_id = g.movie_id
    GROUP BY g.genre
    HAVING AVG(r.average_rating) > 7.0
       AND COUNT(*) > 50
    ORDER BY avg_rating DESC;
    """
    return conn.execute(sql).fetchall()


def query_career_evolution(
    conn: sqlite3.Connection,
    actor_name: str,
) -> List[Tuple[Any, ...]]:
    """
    Évolution de carrière : pour un acteur donné, nombre de films par décennie avec note moyenne.

    Args:
        conn: Connexion SQLite.
        actor_name: Nom (ou partie de nom) de l’acteur.

    Returns:
        Liste de tuples (décennie, nb_films, note_moyenne), triés par décennie croissante.

    SQL utilisé (WITH / CTE) :
        WITH actor_movies AS (
            SELECT DISTINCT m.movie_id,
                            m.start_year
            FROM movies     AS m
            JOIN principals AS p  ON p.movie_id  = m.movie_id
            JOIN persons    AS pe ON pe.person_id = p.person_id
            WHERE pe.name LIKE ?
              AND p.category IN ('actor', 'actress')
              AND m.start_year IS NOT NULL
        ),
        actor_ratings AS (
            SELECT
                am.movie_id,
                (am.start_year / 10) * 10 AS decade,
                r.average_rating
            FROM actor_movies AS am
            LEFT JOIN ratings AS r ON r.movie_id = am.movie_id
        )
        SELECT
            decade,
            COUNT(*)              AS nb_films,
            AVG(average_rating)   AS avg_rating
        FROM actor_ratings
        GROUP BY decade
        ORDER BY decade;
    """
    sql = """
    WITH actor_movies AS (
        SELECT DISTINCT
            m.movie_id,
            m.start_year
        FROM movies     AS m
        JOIN principals AS p  ON p.movie_id  = m.movie_id
        JOIN persons    AS pe ON pe.person_id = p.person_id
        WHERE pe.name LIKE ?
          AND p.category IN ('actor', 'actress')
          AND m.start_year IS NOT NULL
    ),
    actor_ratings AS (
        SELECT
            am.movie_id,
            (am.start_year / 10) * 10 AS decade,
            r.average_rating
        FROM actor_movies AS am
        LEFT JOIN ratings AS r ON r.movie_id = am.movie_id
    )
    SELECT
        decade,
        COUNT(*)            AS nb_films,
        AVG(average_rating) AS avg_rating
    FROM actor_ratings
    GROUP BY decade
    ORDER BY decade;
    """
    return conn.execute(sql, (f"%{actor_name}%",)).fetchall()


def query_top3_by_genre(conn: sqlite3.Connection) -> List[Tuple[Any, ...]]:
    """
    Classement par genre : pour chaque genre, les 3 meilleurs films avec leur rang.

    Returns:
        Liste de tuples (genre, rang, titre, année, note_moyenne).

    SQL utilisé (fenêtre ROW_NUMBER) :
        SELECT genre, rank, primary_title, start_year, average_rating
        FROM (
            SELECT
                g.genre,
                m.primary_title,
                m.start_year,
                r.average_rating,
                ROW_NUMBER() OVER (
                    PARTITION BY g.genre
                    ORDER BY r.average_rating DESC, r.num_votes DESC, m.primary_title ASC
                ) AS rank
            FROM genres  AS g
            JOIN movies  AS m ON m.movie_id = g.movie_id
            JOIN ratings AS r ON r.movie_id = g.movie_id
        )
        WHERE rank <= 3
        ORDER BY genre, rank;
    """
    sql = """
    SELECT
        genre,
        rank,
        primary_title,
        start_year,
        average_rating
    FROM (
        SELECT
            g.genre,
            m.primary_title,
            m.start_year,
            r.average_rating,
            ROW_NUMBER() OVER (
                PARTITION BY g.genre
                ORDER BY r.average_rating DESC,
                         r.num_votes DESC,
                         m.primary_title ASC
            ) AS rank
        FROM genres  AS g
        JOIN movies  AS m ON m.movie_id = g.movie_id
        JOIN ratings AS r ON r.movie_id = g.movie_id
    ) AS sub
    WHERE rank <= 3
    ORDER BY genre, rank;
    """
    return conn.execute(sql).fetchall()


def query_career_boost(conn: sqlite3.Connection) -> List[Tuple[Any, ...]]:
    """
    Carrière propulsée : personnes ayant « percé » grâce à un film.

    Hypothèse :
        - avant : films avec moins de 200k votes
        - après : au moins un film avec 200k votes ou plus
      On retient les personnes ayant au moins un film 'low' et un film 'high'.

    Returns:
        Liste de tuples (nom_personne, nb_films_low, nb_films_high, annee_premier_high),
        triés par nb_films_high décroissant puis par année de percée.

    SQL utilisé :
        WITH person_stats AS (
            SELECT
                p.person_id,
                pe.name,
                SUM(CASE WHEN r.num_votes < 200000 THEN 1 ELSE 0 END) AS low_count,
                SUM(CASE WHEN r.num_votes >= 200000 THEN 1 ELSE 0 END) AS high_count,
                MIN(CASE WHEN r.num_votes >= 200000 THEN m.start_year END) AS breakthrough_year
            FROM principals AS p
            JOIN movies    AS m  ON m.movie_id   = p.movie_id
            JOIN ratings   AS r  ON r.movie_id   = m.movie_id
            JOIN persons   As pe ON pe.person_id = p.person_id
            GROUP BY p.person_id
        )
        SELECT
            name,
            low_count,
            high_count,
            breakthrough_year
        FROM person_stats
        WHERE low_count > 0 AND high_count > 0
        ORDER BY high_count DESC, breakthrough_year;
    """
    sql = """
    WITH person_stats AS (
        SELECT
            p.person_id,
            pe.name,
            SUM(CASE WHEN r.num_votes < 200000 THEN 1 ELSE 0 END)  AS low_count,
            SUM(CASE WHEN r.num_votes >= 200000 THEN 1 ELSE 0 END) AS high_count,
            MIN(CASE WHEN r.num_votes >= 200000 THEN m.start_year END) AS breakthrough_year
        FROM principals AS p
        JOIN movies    AS m  ON m.movie_id   = p.movie_id
        JOIN ratings   AS r  ON r.movie_id   = m.movie_id
        JOIN persons   AS pe ON pe.person_id = p.person_id
        GROUP BY p.person_id
    )
    SELECT
        name,
        low_count,
        high_count,
        breakthrough_year
    FROM person_stats
    WHERE low_count > 0 AND high_count > 0
    ORDER BY high_count DESC, breakthrough_year;
    """
    return conn.execute(sql).fetchall()


def query_most_versatile_actors(
    conn: sqlite3.Connection,
    min_genres: int = 3,
    limit: int = 50,
) -> List[Tuple[Any, ...]]:
    """
    Requête libre : acteurs les plus « polyvalents » en termes de genres joués.

    Args:
        conn: Connexion SQLite.
        min_genres: Nombre minimum de genres distincts pour être retenu.
        limit: Nombre maximal de lignes retournées.

    Returns:
        Liste de tuples (nom_acteur, nb_genres_distincts, nb_films),
        triés par nb_genres décroissant puis nb_films décroissant.

    SQL utilisé (>= 3 jointures) :
        SELECT
            pe.name,
            COUNT(DISTINCT g.genre)    AS nb_genres,
            COUNT(DISTINCT m.movie_id) AS nb_movies
        FROM persons    AS pe
        JOIN principals AS p ON p.person_id = pe.person_id
        JOIN movies     AS m ON m.movie_id  = p.movie_id
        JOIN genres     AS g ON g.movie_id  = m.movie_id
        WHERE p.category IN ('actor', 'actress')
        GROUP BY pe.person_id
        HAVING COUNT(DISTINCT g.genre) >= ?
        ORDER BY nb_genres DESC, nb_movies DESC, pe.name ASC
        LIMIT ?;
    """
    sql = """
    SELECT
        pe.name,
        COUNT(DISTINCT g.genre)    AS nb_genres,
        COUNT(DISTINCT m.movie_id) AS nb_movies
    FROM persons    AS pe
    JOIN principals AS p ON p.person_id = pe.person_id
    JOIN movies     AS m ON m.movie_id  = p.movie_id
    JOIN genres     AS g ON g.movie_id  = m.movie_id
    WHERE p.category IN ('actor', 'actress')
    GROUP BY pe.person_id
    HAVING COUNT(DISTINCT g.genre) >= ?
    ORDER BY nb_genres DESC, nb_movies DESC, pe.name ASC
    LIMIT ?;
    """
    return conn.execute(sql, (min_genres, limit)).fetchall()
