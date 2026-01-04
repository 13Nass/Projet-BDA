# -*- coding: utf-8 -*-
"""
Created on Fri Dec 26 16:40:57 2025

@author: bendr
"""

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pymongo import MongoClient


# -----------------------------
# Helpers (schema-robust)
# -----------------------------
def _first_key(doc: Dict[str, Any], candidates: Sequence[str], ctx: str) -> str:
    """Return the first field name present in doc among candidates."""
    for c in candidates:
        if c in doc:
            return c
    raise KeyError(f"[{ctx}] Aucun champ trouvé parmi {candidates}. Champs dispo: {sorted(doc.keys())}")


def _sample_fields(mdb) -> Dict[str, str]:
    """
    Devine les champs clés en inspectant 1 doc par collection.
    Adapte si ton schéma n'utilise pas exactement movie_id/person_id/etc.
    """
    movies = mdb["movies"].find_one() or {}
    persons = mdb["persons"].find_one() or {}
    ratings = mdb["ratings"].find_one() or {}
    genres = mdb["genres"].find_one() or {}
    principals = mdb["principals"].find_one() or {}
    characters = mdb["characters"].find_one() or {}
    directors = mdb["directors"].find_one() or {}

    f = {}
    # ids
    f["movie_id"] = _first_key(movies, ["movie_id", "id", "tconst", "_id"], "movies.movie_id")
    f["person_id"] = _first_key(persons, ["person_id", "id", "nconst", "_id"], "persons.person_id")

    # text/year
    f["movie_title"] = _first_key(movies, ["title", "primary_title", "name"], "movies.title")
    f["movie_year"] = _first_key(movies, ["year", "start_year", "release_year"], "movies.year")
    f["person_name"] = _first_key(persons, ["name", "primary_name"], "persons.name")

    # relations
    f["genres_movie_id"] = _first_key(genres, ["movie_id", "tconst", f["movie_id"]], "genres.movie_id")
    f["genre"] = _first_key(genres, ["genre", "genres"], "genres.genre")

    f["ratings_movie_id"] = _first_key(ratings, ["movie_id", "tconst", f["movie_id"]], "ratings.movie_id")
    f["avg_rating"] = _first_key(ratings, ["average_rating", "avg_rating", "rating"], "ratings.average_rating")
    f["num_votes"] = _first_key(ratings, ["num_votes", "votes", "number_of_votes"], "ratings.num_votes")

    f["principals_movie_id"] = _first_key(principals, ["movie_id", "tconst", f["movie_id"]], "principals.movie_id")
    f["principals_person_id"] = _first_key(principals, ["person_id", "nconst", f["person_id"]], "principals.person_id")
    f["principals_category"] = _first_key(principals, ["category", "role", "job"], "principals.category")

    # characters table varies a lot
    if characters:
        f["characters_movie_id"] = _first_key(characters, ["movie_id", "tconst", f["movie_id"]], "characters.movie_id")
        f["characters_person_id"] = _first_key(characters, ["person_id", "nconst", f["person_id"]], "characters.person_id")
        f["character_name"] = _first_key(characters, ["character", "characters", "name"], "characters.character")
    else:
        f["characters_movie_id"] = f["movie_id"]
        f["characters_person_id"] = f["person_id"]
        f["character_name"] = "character"

    f["directors_movie_id"] = _first_key(directors, ["movie_id", "tconst", f["movie_id"]], "directors.movie_id")
    f["directors_person_id"] = _first_key(directors, ["person_id", "nconst", f["person_id"]], "directors.person_id")

    return f


def connect_mongo(uri: str, db_name: str):
    client = MongoClient(uri, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    return client, client[db_name]


def _time_ms(fn, *args, repeats: int = 3, warmup: int = 1, **kwargs) -> Tuple[float, Any]:
    """Return (avg_ms, last_result)."""
    last = None
    for _ in range(warmup):
        last = fn(*args, **kwargs)
    t0 = time.perf_counter()
    for _ in range(repeats):
        last = fn(*args, **kwargs)
    t1 = time.perf_counter()
    return ((t1 - t0) * 1000.0) / max(repeats, 1), last


# -----------------------------
# Q1 — Filmographie d’un acteur
# -----------------------------
def q1_actor_filmography(mdb, actor_name: str) -> List[Dict[str, Any]]:
    """
    Dans quels films a joué un acteur donné ?
    Retour: [{title, year, character(s), average_rating}, ...] trié par année desc.
    """
    f = _sample_fields(mdb)

    # trouver les person_id correspondant au nom
    people = list(mdb["persons"].find(
        {f["person_name"]: {"$regex": actor_name, "$options": "i"}},
        {f["person_id"]: 1, f["person_name"]: 1}
    ).limit(10))
    person_ids = [p[f["person_id"]] for p in people]
    if not person_ids:
        return []

    pipeline = [
        {"$match": {
            f["principals_person_id"]: {"$in": person_ids},
            f["principals_category"]: {"$in": ["actor", "actress"]},
        }},
        {"$lookup": {
            "from": "movies",
            "localField": f["principals_movie_id"],
            "foreignField": f["movie_id"],
            "as": "m",
        }},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "ratings",
            "localField": f["principals_movie_id"],
            "foreignField": f["ratings_movie_id"],
            "as": "r",
        }},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        # lookup characters for (movie_id, person_id)
        {"$lookup": {
            "from": "characters",
            "let": {"mid": f"${f['principals_movie_id']}", "pid": f"${f['principals_person_id']}"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": [f"${f['characters_movie_id']}", "$$mid"]},
                    {"$eq": [f"${f['characters_person_id']}", "$$pid"]},
                ]}}},
                {"$project": {f["character_name"]: 1, "_id": 0}},
            ],
            "as": "chars"
        }},
        {"$project": {
            "_id": 0,
            "title": f"$m.{f['movie_title']}",
            "year": f"$m.{f['movie_year']}",
            "average_rating": f"$r.{f['avg_rating']}",
            "characters": {
                "$map": {"input": "$chars", "as": "c", "in": f"$$c.{f['character_name']}"}
            }
        }},
        {"$sort": {"year": -1, "title": 1}}
    ]
    return list(mdb["principals"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Q2 — Top N films d’un genre sur période
# -----------------------------
def q2_top_n_films(mdb, genre: str, year_start: int, year_end: int, n: int) -> List[Dict[str, Any]]:
    f = _sample_fields(mdb)

    pipeline = [
        {"$match": {f["genre"]: genre}},
        {"$lookup": {
            "from": "movies",
            "localField": f["genres_movie_id"],
            "foreignField": f["movie_id"],
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$match": {f"m.{f['movie_year']}": {"$gte": year_start, "$lte": year_end}}},
        {"$lookup": {
            "from": "ratings",
            "localField": f["genres_movie_id"],
            "foreignField": f["ratings_movie_id"],
            "as": "r"
        }},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "title": f"$m.{f['movie_title']}",
            "year": f"$m.{f['movie_year']}",
            "average_rating": f"$r.{f['avg_rating']}",
            "num_votes": f"$r.{f['num_votes']}",
        }},
        {"$sort": {"average_rating": -1, "num_votes": -1, "title": 1}},
        {"$limit": n},
    ]
    return list(mdb["genres"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Q3 — Acteurs multi-rôles (plusieurs personnages dans un même film)
# -----------------------------
def q3_multi_role_actors(mdb, limit: int = 50) -> List[Dict[str, Any]]:
    f = _sample_fields(mdb)

    pipeline = [
        {"$group": {
            "_id": {
                "movie_id": f"${f['characters_movie_id']}",
                "person_id": f"${f['characters_person_id']}",
            },
            "roles": {"$addToSet": f"${f['character_name']}"},
        }},
        {"$addFields": {"role_count": {"$size": "$roles"}}},
        {"$match": {"role_count": {"$gte": 2}}},
        {"$lookup": {
            "from": "persons",
            "localField": "_id.person_id",
            "foreignField": f["person_id"],
            "as": "p"
        }},
        {"$unwind": "$p"},
        {"$lookup": {
            "from": "movies",
            "localField": "_id.movie_id",
            "foreignField": f["movie_id"],
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$project": {
            "_id": 0,
            "actor": f"$p.{f['person_name']}",
            "title": f"$m.{f['movie_title']}",
            "year": f"$m.{f['movie_year']}",
            "role_count": 1,
            "roles": 1,
        }},
        {"$sort": {"role_count": -1, "actor": 1, "year": -1}},
        {"$limit": limit},
    ]
    return list(mdb["characters"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Q4 — Collaborations : réalisateurs ayant travaillé avec un acteur (nb films ensemble)
# -----------------------------
def q4_director_collaborations(mdb, actor_name: str, limit: int = 50) -> List[Dict[str, Any]]:
    f = _sample_fields(mdb)

    people = list(mdb["persons"].find(
        {f["person_name"]: {"$regex": actor_name, "$options": "i"}},
        {f["person_id"]: 1}
    ).limit(10))
    actor_ids = [p[f["person_id"]] for p in people]
    if not actor_ids:
        return []

    pipeline = [
        {"$match": {
            f["principals_person_id"]: {"$in": actor_ids},
            f["principals_category"]: {"$in": ["actor", "actress"]},
        }},
        {"$group": {"_id": f"${f['principals_movie_id']}" }},  # movies where actor played
        {"$lookup": {
            "from": "directors",
            "localField": "_id",
            "foreignField": f["directors_movie_id"],
            "as": "d"
        }},
        {"$unwind": "$d"},
        {"$group": {
            "_id": f"$d.{f['directors_person_id']}",
            "films_together": {"$sum": 1}
        }},
        {"$lookup": {
            "from": "persons",
            "localField": "_id",
            "foreignField": f["person_id"],
            "as": "p"
        }},
        {"$unwind": "$p"},
        {"$project": {
            "_id": 0,
            "director": f"$p.{f['person_name']}",
            "films_together": 1
        }},
        {"$sort": {"films_together": -1, "director": 1}},
        {"$limit": limit}
    ]
    return list(mdb["principals"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Q5 — Genres populaires : avg_rating > 7.0 et plus de 50 films
# -----------------------------
def q5_popular_genres(mdb, min_avg: float = 7.0, min_count: int = 50) -> List[Dict[str, Any]]:
    f = _sample_fields(mdb)

    pipeline = [
        {"$lookup": {
            "from": "ratings",
            "localField": f["genres_movie_id"],
            "foreignField": f["ratings_movie_id"],
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$group": {
            "_id": f"${f['genre']}",
            "film_count": {"$sum": 1},
            "avg_rating": {"$avg": f"$r.{f['avg_rating']}"},
        }},
        {"$match": {"film_count": {"$gt": min_count}, "avg_rating": {"$gt": min_avg}}},
        {"$project": {"_id": 0, "genre": "$_id", "film_count": 1, "avg_rating": 1}},
        {"$sort": {"avg_rating": -1, "film_count": -1, "genre": 1}},
    ]
    return list(mdb["genres"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Q6 — Évolution de carrière : par décennie (nb films + note moyenne)
# -----------------------------
def q6_career_by_decade(mdb, actor_name: str) -> List[Dict[str, Any]]:
    f = _sample_fields(mdb)

    people = list(mdb["persons"].find(
        {f["person_name"]: {"$regex": actor_name, "$options": "i"}},
        {f["person_id"]: 1}
    ).limit(10))
    actor_ids = [p[f["person_id"]] for p in people]
    if not actor_ids:
        return []

    pipeline = [
        {"$match": {
            f["principals_person_id"]: {"$in": actor_ids},
            f["principals_category"]: {"$in": ["actor", "actress"]},
        }},
        {"$lookup": {
            "from": "movies",
            "localField": f["principals_movie_id"],
            "foreignField": f["movie_id"],
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "ratings",
            "localField": f["principals_movie_id"],
            "foreignField": f["ratings_movie_id"],
            "as": "r"
        }},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {
            "decade": {
                "$multiply": [
                    {"$floor": {"$divide": [f"$m.{f['movie_year']}", 10]}},
                    10
                ]
            }
        }},
        {"$group": {
            "_id": "$decade",
            "film_count": {"$sum": 1},
            "avg_rating": {"$avg": f"$r.{f['avg_rating']}"},
        }},
        {"$project": {"_id": 0, "decade": "$_id", "film_count": 1, "avg_rating": 1}},
        {"$sort": {"decade": 1}},
    ]
    return list(mdb["principals"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Q7 — Classement par genre : top 3 films par genre + rang
# (sans $setWindowFields, juste sort/group/slice)
# -----------------------------
def q7_top3_per_genre(mdb) -> List[Dict[str, Any]]:
    f = _sample_fields(mdb)

    pipeline = [
        {"$lookup": {
            "from": "movies",
            "localField": f["genres_movie_id"],
            "foreignField": f["movie_id"],
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "ratings",
            "localField": f["genres_movie_id"],
            "foreignField": f["ratings_movie_id"],
            "as": "r"
        }},
        {"$unwind": {"path": "$r", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "genre": f"${f['genre']}",
            "movie_id": f"${f['genres_movie_id']}",
            "title": f"$m.{f['movie_title']}",
            "year": f"$m.{f['movie_year']}",
            "average_rating": f"$r.{f['avg_rating']}",
            "num_votes": f"$r.{f['num_votes']}",
        }},
        {"$sort": {"genre": 1, "average_rating": -1, "num_votes": -1, "title": 1}},
        {"$group": {"_id": "$genre", "items": {"$push": "$$ROOT"}}},
        {"$project": {"genre": "$_id", "top3": {"$slice": ["$items", 3]}, "_id": 0}},
        {"$unwind": {"path": "$top3", "includeArrayIndex": "rank0"}},
        {"$addFields": {"rank": {"$add": ["$rank0", 1]}}},
        {"$project": {
            "_id": 0,
            "genre": 1,
            "rank": 1,
            "title": "$top3.title",
            "year": "$top3.year",
            "average_rating": "$top3.average_rating",
            "num_votes": "$top3.num_votes",
        }},
        {"$sort": {"genre": 1, "rank": 1}},
    ]
    return list(mdb["genres"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Q8 — Carrière propulsée : avant <200k votes, après >200k votes (chronologique)
# -----------------------------
def q8_breakthrough_people(mdb, threshold_votes: int = 200_000, limit: int = 50) -> List[Dict[str, Any]]:
    f = _sample_fields(mdb)

    pipeline = [
        {"$lookup": {
            "from": "movies",
            "localField": f["principals_movie_id"],
            "foreignField": f["movie_id"],
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "ratings",
            "localField": f["principals_movie_id"],
            "foreignField": f["ratings_movie_id"],
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$project": {
            "person_id": f"${f['principals_person_id']}",
            "year": f"$m.{f['movie_year']}",
            "votes": f"$r.{f['num_votes']}",
        }},
        {"$addFields": {
            "is_high": {"$gte": ["$votes", threshold_votes]},
            "is_low": {"$lt": ["$votes", threshold_votes]},
        }},
        {"$group": {
            "_id": "$person_id",
            "highCount": {"$sum": {"$cond": ["$is_high", 1, 0]}},
            "lowCount": {"$sum": {"$cond": ["$is_low", 1, 0]}},
            "minHighYear": {"$min": {"$cond": ["$is_high", "$year", None]}},
            "maxLowYear": {"$max": {"$cond": ["$is_low", "$year", None]}},
        }},
        {"$match": {
            "highCount": {"$gt": 0},
            "lowCount": {"$gt": 0},
            "$expr": {"$lt": ["$maxLowYear", "$minHighYear"]},
        }},
        {"$lookup": {
            "from": "persons",
            "localField": "_id",
            "foreignField": f["person_id"],
            "as": "p"
        }},
        {"$unwind": "$p"},
        {"$project": {
            "_id": 0,
            "person": f"$p.{f['person_name']}",
            "lowCount": 1,
            "highCount": 1,
            "maxLowYear": 1,
            "minHighYear": 1,
        }},
        {"$sort": {"highCount": -1, "lowCount": -1, "person": 1}},
        {"$limit": limit}
    ]
    return list(mdb["principals"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Q9 — Requête libre (>= 3 jointures) : Top 10 réalisateurs (min 20 films) par note moyenne
# joins: directors -> movies -> ratings -> persons
# -----------------------------
def q9_top_directors(mdb, min_films: int = 20, limit: int = 10) -> List[Dict[str, Any]]:
    f = _sample_fields(mdb)

    pipeline = [
        {"$lookup": {
            "from": "movies",
            "localField": f["directors_movie_id"],
            "foreignField": f["movie_id"],
            "as": "m"
        }},
        {"$unwind": "$m"},
        {"$lookup": {
            "from": "ratings",
            "localField": f["directors_movie_id"],
            "foreignField": f["ratings_movie_id"],
            "as": "r"
        }},
        {"$unwind": "$r"},
        {"$group": {
            "_id": f"${f['directors_person_id']}",
            "film_count": {"$sum": 1},
            "avg_rating": {"$avg": f"$r.{f['avg_rating']}"},
            "best_film": {
                "$top": {
                    "sortBy": {f"r.{f['avg_rating']}": -1, f"r.{f['num_votes']}": -1},
                    "output": {
                        "title": f"$m.{f['movie_title']}",
                        "year": f"$m.{f['movie_year']}",
                        "rating": f"$r.{f['avg_rating']}",
                        "votes": f"$r.{f['num_votes']}",
                    }
                }
            }
        }},
        {"$match": {"film_count": {"$gte": min_films}}},
        {"$lookup": {
            "from": "persons",
            "localField": "_id",
            "foreignField": f["person_id"],
            "as": "p"
        }},
        {"$unwind": "$p"},
        {"$project": {
            "_id": 0,
            "director": f"$p.{f['person_name']}",
            "film_count": 1,
            "avg_rating": 1,
            "best_film": 1,
        }},
        {"$sort": {"avg_rating": -1, "film_count": -1, "director": 1}},
        {"$limit": limit},
    ]
    return list(mdb["directors"].aggregate(pipeline, allowDiskUse=True))


# -----------------------------
# Bench runner
# -----------------------------
def run_benchmarks(mdb, out_csv: Optional[str] = "bench_mongo.csv"):
    # paramètres "raisonnables"
    actor = "Tom Hanks"
    genre = "Drama"

    specs = [
        ("Q1_filmography", lambda: q1_actor_filmography(mdb, actor)),
        ("Q2_topN",        lambda: q2_top_n_films(mdb, genre, 1990, 2000, 10)),
        ("Q3_multiroles",  lambda: q3_multi_role_actors(mdb, 50)),
        ("Q4_collab",      lambda: q4_director_collaborations(mdb, actor, 50)),
        ("Q5_popular",     lambda: q5_popular_genres(mdb, 7.0, 50)),
        ("Q6_decades",     lambda: q6_career_by_decade(mdb, actor)),
        ("Q7_top3genre",   lambda: q7_top3_per_genre(mdb)),
        ("Q8_breakthrough",lambda: q8_breakthrough_people(mdb, 200_000, 50)),
        ("Q9_free",        lambda: q9_top_directors(mdb, 20, 10)),
    ]

    rows = []
    print("=== MongoDB benchmark (avg ms) ===")
    for name, thunk in specs:
        ms, res = _time_ms(lambda: list(thunk()), repeats=3, warmup=1)
        size = len(res) if isinstance(res, list) else None
        print(f"{name:15s} {ms:10.2f} ms   (rows={size})")
        rows.append({"query": name, "mongo_ms_avg": round(ms, 3), "rows": size})

    if out_csv:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["query", "mongo_ms_avg", "rows"])
            w.writeheader()
            w.writerows(rows)
        print(f"\nCSV écrit: {out_csv}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="cineexplorer_flat")  # ta DB migrée
    ap.add_argument("--bench", action="store_true")
    args = ap.parse_args()

    client, mdb = connect_mongo(args.mongo_uri, args.mongo_db)

    if args.bench:
        run_benchmarks(mdb, out_csv="bench_mongo.csv")
    else:
        # petit test rapide
        print(q1_actor_filmography(mdb, "Tom Hanks")[:3])

    client.close()


if __name__ == "__main__":
    main()
