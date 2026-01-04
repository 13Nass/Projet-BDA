# -*- coding: utf-8 -*-
"""
Created on Fri Dec 26 18:05:22 2025

@author: bendr
"""

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import sqlite3
from pymongo import MongoClient


# -----------------------------------------------------------------------------
# Paths / imports Phase 1 (SQLite)
# -----------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
PHASE1_DIR = ROOT_DIR / "script" / "phase1_sqlite"
sys.path.insert(0, str(PHASE1_DIR))

# réutilise ton bench et tes requêtes Phase 1
import benchmark_sqlite as sqlite_bench  # type: ignore


# -----------------------------------------------------------------------------
# Timing helper
# -----------------------------------------------------------------------------
def time_ms(thunk: Callable[[], Any], repeats: int = 3, warmup: int = 1) -> Tuple[float, Any]:
    last = None
    for _ in range(warmup):
        last = thunk()
    t0 = time.perf_counter()
    for _ in range(repeats):
        last = thunk()
        # force matérialisation si list/tuple
        try:
            _ = len(last)  # noqa
        except Exception:
            pass
    t1 = time.perf_counter()
    return ((t1 - t0) * 1000.0) / max(repeats, 1), last


# -----------------------------------------------------------------------------
# Mongo queries (équivalents de script/phase1_sqlite/queries.py)
# Schéma connu via create_schema.py : movie_id, primary_title, start_year, etc. :contentReference[oaicite:1]{index=1}
# -----------------------------------------------------------------------------
def _find_person_ids(mdb, name: str, limit: int = 20) -> List[str]:
    cur = mdb["persons"].find(
        {"name": {"$regex": name, "$options": "i"}},
        {"person_id": 1, "_id": 0},
    ).limit(limit)
    return [d["person_id"] for d in cur]


def mongo_q1_actor_filmography(mdb, actor_name: str) -> List[Dict[str, Any]]:
    # équivalent query_actor_filmography :contentReference[oaicite:2]{index=2}
    person_ids = _find_person_ids(mdb, actor_name)
    if not person_ids:
        return []

    pipeline = [
        {"$match": {"person_id": {"$in": person_ids}, "category": {"$in": ["actor", "actress"]}}},
        {"$lookup": {"from": "movies", "localField": "movie_id", "foreignField": "movie_id", "as": "m"}},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "ratings",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "r"
        }},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "characters",
            "let": {"mid": "$movie_id", "pid": "$person_id"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$movie_id", "$$mid"]},
                    {"$eq": ["$person_id", "$$pid"]},
                ]}}},
                {"$project": {"_id": 0, "name": 1}}
            ],
            "as": "c"
        }},
        # on sort une ligne par personnage (comme la jointure SQL characters)
        {"$unwind": {"path": "$c", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "primary_title": "$m.primary_title",
            "start_year": "$m.start_year",
            "character": "$c.name",
            "average_rating": "$r.average_rating",
        }},
        {"$sort": {"start_year": -1, "primary_title": 1}},
    ]
    return list(mdb["principals"].aggregate(pipeline, allowDiskUse=True))


def mongo_q2_top_n_movies(mdb, genre: str, start_year: int, end_year: int, n: int) -> List[Dict[str, Any]]:
    # équivalent query_top_n_movies :contentReference[oaicite:3]{index=3}
    pipeline = [
        {"$match": {"genre": genre}},
        {"$lookup": {"from": "movies", "localField": "movie_id", "foreignField": "movie_id", "as": "m"}},
        {"$unwind": "$m"},
        {"$match": {"m.start_year": {"$gte": start_year, "$lte": end_year}}},
        {"$lookup": {"from": "ratings", "localField": "movie_id", "foreignField": "movie_id", "as": "r"}},
        {"$unwind": "$r"},
        {"$project": {
            "_id": 0,
            "primary_title": "$m.primary_title",
            "start_year": "$m.start_year",
            "average_rating": "$r.average_rating",
            "num_votes": "$r.num_votes",
        }},
        {"$sort": {"average_rating": -1, "num_votes": -1, "primary_title": 1}},
        {"$limit": n},
    ]
    return list(mdb["genres"].aggregate(pipeline, allowDiskUse=True))


def mongo_q3_multi_role_actors_fast(mdb, limit: int = 200):
    pipeline = [
        {"$group": {
            "_id": {"movie_id": "$movie_id", "person_id": "$person_id"},
            "nb_roles": {"$sum": 1}
        }},
        {"$match": {"nb_roles": {"$gt": 1}}},
        {"$sort": {"nb_roles": -1}},
        {"$limit": limit},  # IMPORTANT: on limite AVANT les lookups

        {"$lookup": {"from": "persons", "localField": "_id.person_id", "foreignField": "person_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$lookup": {"from": "movies", "localField": "_id.movie_id", "foreignField": "movie_id", "as": "m"}},
        {"$unwind": "$m"},

        {"$project": {"_id": 0, "name": "$p.name", "primary_title": "$m.primary_title",
                      "start_year": "$m.start_year", "nb_roles": 1}},
        {"$sort": {"nb_roles": -1, "name": 1}},
    ]
    return list(mdb["characters"].aggregate(pipeline, allowDiskUse=True))



def mongo_q4_collaborations(mdb, actor_name: str) -> List[Dict[str, Any]]:
    # équivalent query_collaborations :contentReference[oaicite:5]{index=5}
    person_ids = _find_person_ids(mdb, actor_name)
    if not person_ids:
        return []

    pipeline = [
        {"$match": {"person_id": {"$in": person_ids}, "category": {"$in": ["actor", "actress"]}}},
        {"$group": {"_id": "$movie_id"}},  # DISTINCT movie_id
        {"$lookup": {"from": "directors", "localField": "_id", "foreignField": "movie_id", "as": "d"}},
        {"$unwind": "$d"},
        {"$group": {"_id": "$d.person_id", "nb_films": {"$sum": 1}}},
        {"$lookup": {"from": "persons", "localField": "_id", "foreignField": "person_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {"_id": 0, "director_name": "$p.name", "nb_films": 1}},
        {"$sort": {"nb_films": -1, "director_name": 1}},
    ]
    return list(mdb["principals"].aggregate(pipeline, allowDiskUse=True))


def mongo_q5_popular_genres(mdb) -> List[Dict[str, Any]]:
    # équivalent query_popular_genres :contentReference[oaicite:6]{index=6}
    pipeline = [
        {"$lookup": {"from": "ratings", "localField": "movie_id", "foreignField": "movie_id", "as": "r"}},
        {"$unwind": "$r"},
        {"$group": {
            "_id": "$genre",
            "nb_films": {"$sum": 1},
            "avg_rating": {"$avg": "$r.average_rating"},
        }},
        {"$match": {"avg_rating": {"$gt": 7.0}, "nb_films": {"$gt": 50}}},
        {"$project": {"_id": 0, "genre": "$_id", "nb_films": 1, "avg_rating": 1}},
        {"$sort": {"avg_rating": -1}},
    ]
    return list(mdb["genres"].aggregate(pipeline, allowDiskUse=True))


def mongo_q6_career_evolution(mdb, actor_name: str) -> List[Dict[str, Any]]:
    # équivalent query_career_evolution :contentReference[oaicite:7]{index=7}
    person_ids = _find_person_ids(mdb, actor_name)
    if not person_ids:
        return []

    pipeline = [
        {"$match": {"person_id": {"$in": person_ids}, "category": {"$in": ["actor", "actress"]}}},
        {"$lookup": {"from": "movies", "localField": "movie_id", "foreignField": "movie_id", "as": "m"}},
        {"$unwind": "$m"},
        {"$match": {"m.start_year": {"$ne": None}}},
        # DISTINCT movie_id pour éviter doublons (équivalent du DISTINCT SQL)
        {"$group": {"_id": "$movie_id", "start_year": {"$first": "$m.start_year"}}},
        {"$lookup": {"from": "ratings", "localField": "_id", "foreignField": "movie_id", "as": "r"}},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {"decade": {"$multiply": [{"$floor": {"$divide": ["$start_year", 10]}}, 10]}}},
        {"$group": {
            "_id": "$decade",
            "nb_films": {"$sum": 1},
            "avg_rating": {"$avg": "$r.average_rating"},
        }},
        {"$project": {"_id": 0, "decade": "$_id", "nb_films": 1, "avg_rating": 1}},
        {"$sort": {"decade": 1}},
    ]
    return list(mdb["principals"].aggregate(pipeline, allowDiskUse=True))


def mongo_q7_top3_by_genre(mdb) -> List[Dict[str, Any]]:
    # équivalent query_top3_by_genre :contentReference[oaicite:8]{index=8}
    pipeline = [
        {"$lookup": {"from": "movies", "localField": "movie_id", "foreignField": "movie_id", "as": "m"}},
        {"$unwind": "$m"},
        {"$lookup": {"from": "ratings", "localField": "movie_id", "foreignField": "movie_id", "as": "r"}},
        {"$unwind": "$r"},
        {"$project": {
            "_id": 0,
            "genre": "$genre",
            "primary_title": "$m.primary_title",
            "start_year": "$m.start_year",
            "average_rating": "$r.average_rating",
            "num_votes": "$r.num_votes",
        }},
        {"$sort": {"genre": 1, "average_rating": -1, "num_votes": -1, "primary_title": 1}},
        {"$group": {"_id": "$genre", "items": {"$push": "$$ROOT"}}},
        {"$project": {"genre": "$_id", "top3": {"$slice": ["$items", 3]}, "_id": 0}},
        {"$unwind": {"path": "$top3", "includeArrayIndex": "rank0"}},
        {"$addFields": {"rank": {"$add": ["$rank0", 1]}}},
        {"$project": {
            "_id": 0,
            "genre": 1,
            "rank": 1,
            "primary_title": "$top3.primary_title",
            "start_year": "$top3.start_year",
            "average_rating": "$top3.average_rating",
        }},
        {"$sort": {"genre": 1, "rank": 1}},
    ]
    return list(mdb["genres"].aggregate(pipeline, allowDiskUse=True))


def mongo_q8_career_boost_fast2(mdb, threshold: int = 200_000):
    pipeline = [
        # base: ratings (≈ 36k)
        {"$project": {"_id": 0, "movie_id": 1, "num_votes": 1}},

        # join movies pour start_year (1 lookup par movie)
        {"$lookup": {
            "from": "movies",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "pipeline": [{"$project": {"_id": 0, "start_year": 1}}],
            "as": "m"
        }},
        {"$unwind": "$m"},

        # join principals pour obtenir les personnes liées au film
        {"$lookup": {
            "from": "principals",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "pipeline": [{"$project": {"_id": 0, "person_id": 1}}],
            "as": "p"
        }},
        {"$unwind": "$p"},

        # group par person_id (exactement comme le SQL) :contentReference[oaicite:1]{index=1}
        {"$group": {
            "_id": "$p.person_id",
            "low_count": {"$sum": {"$cond": [{"$lt": ["$num_votes", threshold]}, 1, 0]}},
            "high_count": {"$sum": {"$cond": [{"$gte": ["$num_votes", threshold]}, 1, 0]}},
            "breakthrough_year": {"$min": {"$cond": [{"$gte": ["$num_votes", threshold]}, "$m.start_year", None]}},
        }},
        {"$match": {"low_count": {"$gt": 0}, "high_count": {"$gt": 0}}},

        # join persons à la fin (après réduction)
        {"$lookup": {"from": "persons", "localField": "_id", "foreignField": "person_id", "as": "pe"}},
        {"$unwind": "$pe"},
        {"$project": {"_id": 0, "name": "$pe.name", "low_count": 1, "high_count": 1, "breakthrough_year": 1}},
        {"$sort": {"high_count": -1, "breakthrough_year": 1}},
    ]
    return list(mdb["ratings"].aggregate(pipeline, allowDiskUse=True))



def mongo_q9_most_versatile_actors(mdb, min_genres: int = 3, limit: int = 50) -> List[Dict[str, Any]]:
    # équivalent query_most_versatile_actors :contentReference[oaicite:10]{index=10}
    pipeline = [
        {"$match": {"category": {"$in": ["actor", "actress"]}}},
        # DISTINCT (person_id, movie_id)
        {"$group": {"_id": {"person_id": "$person_id", "movie_id": "$movie_id"}}},
        {"$lookup": {"from": "genres", "localField": "_id.movie_id", "foreignField": "movie_id", "as": "g"}},
        {"$unwind": "$g"},
        {"$group": {
            "_id": "$_id.person_id",
            "genres": {"$addToSet": "$g.genre"},
            "movies": {"$addToSet": "$g.movie_id"},
        }},
        {"$addFields": {"nb_genres": {"$size": "$genres"}, "nb_movies": {"$size": "$movies"}}},
        {"$match": {"nb_genres": {"$gte": min_genres}}},
        {"$lookup": {"from": "persons", "localField": "_id", "foreignField": "person_id", "as": "p"}},
        {"$unwind": "$p"},
        {"$project": {"_id": 0, "name": "$p.name", "nb_genres": 1, "nb_movies": 1}},
        {"$sort": {"nb_genres": -1, "nb_movies": -1, "name": 1}},
        {"$limit": limit},
    ]
    return list(mdb["principals"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------------------------------------------------------
# Indexes SQLite (optionnel) : même liste que ton bench Phase1 :contentReference[oaicite:11]{index=11}
# -----------------------------------------------------------------------------
def apply_sqlite_indexes(conn: sqlite3.Connection):
    cur = conn.cursor()
    for _, ddl in sqlite_bench.INDEXES:
        cur.execute(ddl)
    conn.commit()


# -----------------------------------------------------------------------------
# Main compare
# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sqlite", default=str(ROOT_DIR / "data" / "imdb.db"))
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="cineexplorer_flat")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--with-indexes", action="store_true", help="Applique les index SQLite avant bench (comme phase 1).")
    ap.add_argument("--out", default="bench_compare.csv")
    args = ap.parse_args()

    # SQLite
    conn = sqlite3.connect(args.sqlite)
    conn.execute("PRAGMA foreign_keys = ON;")

    if args.with_indexes:
        apply_sqlite_indexes(conn)

    sqlite_specs = sqlite_bench.make_queries()  # labels identiques à ton bench :contentReference[oaicite:12]{index=12}

    # Mongo
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    mdb = client[args.mongo_db]

    mongo_specs: Dict[str, Callable[[], List[Dict[str, Any]]]] = {
        "Q1 - Filmographie (Tom Hanks)": lambda: mongo_q1_actor_filmography(mdb, "Tom Hanks"),
        "Q2 - Top 50 Drama 1990-2020": lambda: mongo_q2_top_n_movies(mdb, "Drama", 1990, 2020, 50),
        "Q3 - Acteurs multi-rôles": lambda: mongo_q3_multi_role_actors_fast(mdb, limit=200),
        "Q4 - Collaborations (Tom Hanks)": lambda: mongo_q4_collaborations(mdb, "Tom Hanks"),
        "Q5 - Genres populaires": lambda: mongo_q5_popular_genres(mdb),
        "Q6 - Carrière (Tom Hanks)": lambda: mongo_q6_career_evolution(mdb, "Tom Hanks"),
        "Q7 - Top 3 par genre": lambda: mongo_q7_top3_by_genre(mdb),
        "Q8 - Carrières boostées": lambda: mongo_q8_career_boost_fast2(mdb),
        "Q9 - Acteurs polyvalents": lambda: mongo_q9_most_versatile_actors(mdb, min_genres=3, limit=50),
    }

    rows_out = []
    print("\n=== Benchmark compare SQLite vs MongoDB (avg ms) ===")
    print(f"SQLite DB: {args.sqlite}")
    print(f"Mongo DB : {args.mongo_db}")
    print(f"Indexes  : {'ON' if args.with_indexes else 'OFF'}")
    print("-" * 78)
    print(f"{'Requête':38s} {'SQLite(ms)':>12s} {'Mongo(ms)':>12s} {'RowsSQL':>8s} {'RowsMongo':>9s}")
    print("-" * 78)

    for label, sql_fn in sqlite_specs.items():
        if label not in mongo_specs:
            print(f"[SKIP] pas d'équivalent Mongo défini pour: {label}")
            continue

        s_ms, s_res = time_ms(lambda: sql_fn(conn), repeats=args.repeats, warmup=args.warmup)
        m_ms, m_res = time_ms(lambda: mongo_specs[label](), repeats=args.repeats, warmup=args.warmup)

        n_sql = len(s_res) if isinstance(s_res, list) else None
        n_m = len(m_res) if isinstance(m_res, list) else None

        print(f"{label[:38]:38s} {s_ms:12.2f} {m_ms:12.2f} {str(n_sql):>8s} {str(n_m):>9s}")

        rows_out.append({
            "query": label,
            "sqlite_ms_avg": round(s_ms, 3),
            "mongo_ms_avg": round(m_ms, 3),
            "rows_sqlite": n_sql,
            "rows_mongo": n_m,
            "sqlite_indexes": int(args.with_indexes),
        })

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        w.writeheader()
        w.writerows(rows_out)

    print(f"\nCSV écrit: {args.out}")

    conn.close()
    client.close()


if __name__ == "__main__":
    main()
