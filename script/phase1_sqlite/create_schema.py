# scripts/phase1_sqlite/create_schema.py

import sqlite3
from pathlib import Path

# Emplacement de la base SQLite : cineexplorer/data/imdb.db
DB_PATH = Path(__file__).resolve().parents[2] / "data" / "imdb.db"


DDL_SCRIPT = """
PRAGMA foreign_keys = OFF;

-- On supprime les tables si elles existent déjà (ordre enfants -> parents)
DROP TABLE IF EXISTS titles;
DROP TABLE IF EXISTS professions;
DROP TABLE IF EXISTS writers;
DROP TABLE IF EXISTS directors;
DROP TABLE IF EXISTS principals;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS knownformovies;
DROP TABLE IF EXISTS characters;
DROP TABLE IF EXISTS persons;
DROP TABLE IF EXISTS movies;

PRAGMA foreign_keys = ON;

-----------------------------------------------------------
-- TABLES PRINCIPALES
-----------------------------------------------------------

CREATE TABLE movies (
    movie_id        TEXT PRIMARY KEY,
    title_type      TEXT NOT NULL,
    primary_title   TEXT NOT NULL,
    original_title  TEXT,
    is_adult        INTEGER NOT NULL,
    start_year      INTEGER,
    end_year        INTEGER,
    runtime_minutes INTEGER
);

CREATE TABLE persons (
    person_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    birth_year  INTEGER,
    death_year  INTEGER
);

-----------------------------------------------------------
-- RELATIONS AUTOUR DES FILMS ET PERSONNES
-----------------------------------------------------------

-- Personnages joués par les acteurs
CREATE TABLE characters (
    movie_id   TEXT NOT NULL,
    person_id  TEXT NOT NULL,
    name       TEXT NOT NULL,      -- nom du personnage
    PRIMARY KEY (movie_id, person_id, name),
    FOREIGN KEY (movie_id)  REFERENCES movies(movie_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Films pour lesquels une personne est principalement connue
CREATE TABLE knownformovies (
    movie_id  TEXT NOT NULL,
    person_id TEXT NOT NULL,
    PRIMARY KEY (movie_id, person_id),
    FOREIGN KEY (movie_id)  REFERENCES movies(movie_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Genres associés aux films
CREATE TABLE genres (
    movie_id TEXT NOT NULL,
    genre    TEXT NOT NULL,
    PRIMARY KEY (movie_id, genre),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Notes et nombre de votes
CREATE TABLE ratings (
    movie_id       TEXT PRIMARY KEY,
    average_rating REAL NOT NULL,
    num_votes      INTEGER NOT NULL,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Participants principaux d'un film
CREATE TABLE principals (
    movie_id  TEXT NOT NULL,
    person_id TEXT NOT NULL,
    ordering  INTEGER NOT NULL,
    category  TEXT NOT NULL,
    job       TEXT,
    PRIMARY KEY (movie_id, person_id, ordering),
    FOREIGN KEY (movie_id)  REFERENCES movies(movie_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Réalisateurs
CREATE TABLE directors (
    movie_id  TEXT NOT NULL,
    person_id TEXT NOT NULL,
    PRIMARY KEY (movie_id, person_id),
    FOREIGN KEY (movie_id)  REFERENCES movies(movie_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Scénaristes
CREATE TABLE writers (
    movie_id  TEXT NOT NULL,
    person_id TEXT NOT NULL,
    PRIMARY KEY (movie_id, person_id),
    FOREIGN KEY (movie_id)  REFERENCES movies(movie_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Professions d'une personne (une personne peut avoir plusieurs jobs)
CREATE TABLE professions (
    person_id TEXT NOT NULL,
    job_name  TEXT NOT NULL,
    PRIMARY KEY (person_id, job_name),
    FOREIGN KEY (person_id) REFERENCES persons(person_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Titres alternatifs par région
CREATE TABLE titles (
    movie_id TEXT NOT NULL,
    region   TEXT NOT NULL,
    title    TEXT NOT NULL,
    PRIMARY KEY (movie_id, region),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);
"""


def create_schema(db_path: Path = DB_PATH) -> None:
    """
    Crée (ou recrée) le schéma SQLite imdb.db pour la Phase 1.
    Toutes les tables sont supprimées puis recréées.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.executescript(DDL_SCRIPT)
        conn.commit()
        print(f"Schéma SQLite créé dans {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    create_schema()
