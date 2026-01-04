# script/phase1_sqlite/import_data.py

from pathlib import Path
import sqlite3
import pandas as pd
from typing import List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
DB_PATH = ROOT_DIR / "data" / "imdb.db"
CSV_DIR = ROOT_DIR / "data" / "csv"


def find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    for cand in candidates:
        for c in cols:
            if cand in c:
                return c
    return None


def import_movies(conn):
    # lire le csv
    df = pd.read_csv(CSV_DIR / "movies.csv")
    # on liste les colonnes de la donnée
    cols = list(df.columns)
    
    # vu qu'on connait déjà les colonnes de movies grâce
    # à l'explorateur.pydmb on va créer chaque colonne en les cherchant
    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])
    ttype_col = find_col(cols, ["titleType", "title_type"])
    ptitle_col = find_col(cols, ["primaryTitle", "primary_title"])
    otitle_col = find_col(cols, ["originalTitle", "original_title"])
    adult_col = find_col(cols, ["isAdult", "is_adult"])
    start_col = find_col(cols, ["startYear", "start_year"])
    end_col = find_col(cols, ["endYear", "end_year"])
    runtime_col = find_col(cols, ["runtimeMinutes", "runtime_minutes"])
    
    #une fois trouver on définit leur taille
    movies = pd.DataFrame()
    movies["movie_id"] = df[mid_col]
    movies["title_type"] = df[ttype_col]
    movies["primary_title"] = df[ptitle_col]
    movies["original_title"] = df[otitle_col]
    movies["is_adult"] = pd.to_numeric(df[adult_col], errors="coerce").fillna(0).astype(int)
    movies["start_year"] = pd.to_numeric(df[start_col], errors="coerce")
    movies["end_year"] = pd.to_numeric(df[end_col], errors="coerce")
    movies["runtime_minutes"] = pd.to_numeric(df[runtime_col], errors="coerce")

    #on transforme la donnée movie en donnée sql
    movies.to_sql("movies", conn, if_exists="append", index=False)
    print(f"[movies] insérés={len(movies)}, erreurs=0")

# pareil ici
def import_persons(conn):
    df = pd.read_csv(CSV_DIR / "persons.csv")
    cols = list(df.columns)

    pid_col = find_col(cols, ["pid", "person_id", "nconst"])
    name_col = find_col(cols, ["primaryName", "name"])
    birth_col = find_col(cols, ["birthYear", "birth_year"])
    death_col = find_col(cols, ["deathYear", "death_year"])

    persons = pd.DataFrame()
    persons["person_id"] = df[pid_col]
    persons["name"] = df[name_col]
    persons["birth_year"] = pd.to_numeric(df[birth_col], errors="coerce")
    persons["death_year"] = pd.to_numeric(df[death_col], errors="coerce")

    persons.to_sql("persons", conn, if_exists="append", index=False)
    print(f"[persons] insérés={len(persons)}")

# ici j'ai rencontré des difficulté sur la recherche du csv
# et les clés etrangères et primaire à la table
# et les valeurs null
def import_professions(conn):
    path = CSV_DIR / "professions.csv"
    if not path.exists():
        print("[professions] fichier absent -> 0 ligne insérée")
        return

    df = pd.read_csv(path)
    cols = list(df.columns)

    pid_col = find_col(cols, ["pid", "person_id", "nconst"])
    job_col = find_col(cols, ["jobName", "job_name", "job"])

    prof = pd.DataFrame()
    prof["person_id"] = df[pid_col]

    # Nettoyage pour éviter les NULL sur job_name
    prof["job_name"] = df[job_col].astype(str).str.strip()
    prof = prof[prof["job_name"].notna()]
    prof = prof[prof["job_name"] != ""]
    prof = prof.drop_duplicates(subset=["person_id", "job_name"])

    prof.to_sql("professions", conn, if_exists="append", index=False)
    print(f"[professions] insérés={len(prof)}")

# pareil ici
def import_knownformovies(conn):
    path = CSV_DIR / "knownformovies.csv"
    if not path.exists():
        print("[knownformovies] fichier absent -> 0 ligne insérée")
        return

    df = pd.read_csv(path)
    cols = list(df.columns)

    pid_col = find_col(cols, ["pid", "person_id", "nconst"])
    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])

    kfm = pd.DataFrame()
    kfm["person_id"] = df[pid_col]
    kfm["movie_id"] = df[mid_col]

    kfm.to_sql("knownformovies", conn, if_exists="append", index=False)
    print(f"[knownformovies] insérés={len(kfm)}")

#pareil ici
def import_genres(conn):
    df = pd.read_csv(CSV_DIR / "genres.csv")
    cols = list(df.columns)

    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])
    genre_col = find_col(cols, ["genre"])

    g = pd.DataFrame()
    g["movie_id"] = df[mid_col]
    g["genre"] = df[genre_col].astype(str).str.rstrip(",")

    g.to_sql("genres", conn, if_exists="append", index=False)
    print(f"[genres] insérés={len(g)}")

#pareil ici
def import_ratings(conn):
    df = pd.read_csv(CSV_DIR / "ratings.csv")
    cols = list(df.columns)

    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])
    avg_col = find_col(cols, ["averageRating", "average_rating"])
    votes_col = find_col(cols, ["numVotes", "num_votes"])

    r = pd.DataFrame()
    r["movie_id"] = df[mid_col]
    r["average_rating"] = pd.to_numeric(df[avg_col], errors="coerce")
    r["num_votes"] = pd.to_numeric(df[votes_col], errors="coerce").fillna(0).astype(int)

    r.to_sql("ratings", conn, if_exists="append", index=False)
    print(f"[ratings] insérés={len(r)}")

#ici des erreurs de doublons de clés primaires sont survenus
def import_titles(conn):
    df = pd.read_csv(CSV_DIR / "titles.csv")
    cols = list(df.columns)

    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])
    region_col = find_col(cols, ["region", "Region"])
    title_col = find_col(cols, ["title", "Title"])

    t = pd.DataFrame()
    t["movie_id"] = df[mid_col]
    t["region"] = df[region_col]
    t["title"] = df[title_col]

    # nettoyer / filtrer
    t["movie_id"] = t["movie_id"].astype(str).str.strip()
    t["region"] = t["region"].astype(str).str.strip()
    t["title"] = t["title"].astype(str).str.strip()

    t = t[(t["movie_id"] != "") & t["movie_id"].notna()]
    t = t[(t["region"] != "") & t["region"].notna()]
    t = t[(t["title"] != "") & t["title"].notna()]

    # éviter les doublons PK (movie_id, region)
    t = t.drop_duplicates(subset=["movie_id", "region"])

    t.to_sql("titles", conn, if_exists="append", index=False)
    print(f"[titles] insérés={len(t)}")


def import_directors(conn):
    df = pd.read_csv(CSV_DIR / "directors.csv")
    cols = list(df.columns)

    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])
    pid_col = find_col(cols, ["pid", "person_id", "nconst"])

    d = pd.DataFrame()
    d["movie_id"] = df[mid_col]
    d["person_id"] = df[pid_col]

    d.to_sql("directors", conn, if_exists="append", index=False)
    print(f"[directors] insérés={len(d)}")


def import_writers(conn):
    path = CSV_DIR / "writers.csv"
    if not path.exists():
        print("[writers] fichier absent -> 0 ligne insérée")
        return

    df = pd.read_csv(path)
    cols = list(df.columns)

    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])
    pid_col = find_col(cols, ["pid", "person_id", "nconst"])

    w = pd.DataFrame()
    w["movie_id"] = df[mid_col]
    w["person_id"] = df[pid_col]

    # Nettoyage basique
    w["movie_id"] = w["movie_id"].astype(str).str.strip()
    w["person_id"] = w["person_id"].astype(str).str.strip()
    w = w[(w["movie_id"] != "") & (w["person_id"] != "")]
    w = w.drop_duplicates(subset=["movie_id", "person_id"])

    # Garder uniquement les lignes dont les FK existent
    movies_ids = pd.read_sql("SELECT movie_id FROM movies;", conn)["movie_id"]
    persons_ids = pd.read_sql("SELECT person_id FROM persons;", conn)["person_id"]

    before = len(w)
    w = w[w["movie_id"].isin(movies_ids) & w["person_id"].isin(persons_ids)]
    after = len(w)

    w.to_sql("writers", conn, if_exists="append", index=False)
    print(f"[writers] insérés={after}, ignorés={before - after}")



def import_principals(conn):
    df = pd.read_csv(CSV_DIR / "principals.csv")
    cols = list(df.columns)

    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])
    ord_col = find_col(cols, ["ordering"])
    pid_col = find_col(cols, ["pid", "person_id", "nconst"])
    cat_col = find_col(cols, ["category"])
    job_col = find_col(cols, ["job"])

    p = pd.DataFrame()
    p["movie_id"] = df[mid_col].astype(str).str.strip()
    p["ordering"] = pd.to_numeric(df[ord_col], errors="coerce").fillna(0).astype(int)
    p["person_id"] = df[pid_col].astype(str).str.strip()
    p["category"] = df[cat_col]
    p["job"] = df[job_col]

    # virer lignes vides
    p = p[(p["movie_id"] != "") & (p["person_id"] != "")]
    # enlever doublons PK
    p = p.drop_duplicates(subset=["movie_id", "person_id", "ordering"])

    cur = conn.cursor()

    # table temporaire
    cur.execute("DROP TABLE IF EXISTS tmp_principals;")
    conn.commit()
    p.to_sql("tmp_principals", conn, if_exists="replace", index=False)

    # compteur avant
    before = cur.execute("SELECT COUNT(*) FROM principals;").fetchone()[0]

    # insertion en ne gardant que les lignes avec FK valides
    cur.execute(
        """
        INSERT OR IGNORE INTO principals(movie_id, person_id, ordering, category, job)
        SELECT t.movie_id, t.person_id, t.ordering, t.category, t.job
        FROM tmp_principals t
        JOIN movies m   ON m.movie_id   = t.movie_id
        JOIN persons pe ON pe.person_id = t.person_id;
        """
    )
    conn.commit()

    after = cur.execute("SELECT COUNT(*) FROM principals;").fetchone()[0]
    inserted = after - before

    print(f"[principals] insérés={inserted}")



def import_characters(conn):
    df = pd.read_csv(CSV_DIR / "characters.csv")
    cols = list(df.columns)

    mid_col = find_col(cols, ["mid", "tconst", "movie_id"])
    pid_col = find_col(cols, ["pid", "person_id", "nconst"])
    name_col = find_col(cols, ["name", "character"])

    c = pd.DataFrame()
    c["movie_id"] = df[mid_col].astype(str).str.strip()
    c["person_id"] = df[pid_col].astype(str).str.strip()
    c["name"] = df[name_col].astype(str).str.strip()

    # virer lignes vides
    c = c[(c["movie_id"] != "") & (c["person_id"] != "") & (c["name"] != "")]
    # enlever doublons internes
    c = c.drop_duplicates(subset=["movie_id", "person_id", "name"])

    # garder uniquement FK valides
    movies_ids = pd.read_sql("SELECT movie_id FROM movies;", conn)["movie_id"]
    persons_ids = pd.read_sql("SELECT person_id FROM persons;", conn)["person_id"]

    c = c[c["movie_id"].isin(movies_ids) & c["person_id"].isin(persons_ids)]

    # on vide la table pour éviter les conflits avec des données déjà présentes
    cur = conn.cursor()
    cur.execute("DELETE FROM characters")
    conn.commit()

    # insertion avec IGNORE pour éviter les derniers doublons éventuels
    cur.executemany(
        "INSERT OR IGNORE INTO characters(movie_id, person_id, name) VALUES (?, ?, ?)",
        list(c.itertuples(index=False, name=None)),
    )
    conn.commit()
    print(f"[characters] insérés={len(c)}")



def main():
    print(f"Import des données depuis {CSV_DIR} vers {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        import_movies(conn)
        import_persons(conn)
        import_professions(conn)
        import_knownformovies(conn)
        import_genres(conn)
        import_ratings(conn)
        import_titles(conn)
        import_directors(conn)
        import_writers(conn)
        import_principals(conn)
        import_characters(conn)
    finally:
        conn.close()
        print("✅ Import terminé.")


if __name__ == "__main__":
    main()
