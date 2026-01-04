from pymongo import MongoClient

# ----------------------------
# Config
# ----------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "cineexplorer_flat"
OUT_COL = "movies_complete"

# Pour tester vite sur 1 film (recommandé au début):
# TEST_MOVIE_ID = "tt0111161"
# None pour toute la collection
TEST_MOVIE_ID = None
LIMIT_MOVIES = 20

# ----------------------------
# Connexion
# ----------------------------
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
client.admin.command("ping")
db = client[DB_NAME]

# ----------------------------
# Index utiles (safe à relancer)
# ----------------------------
db.persons.create_index([("person_id", 1)])
db.movies.create_index([("movie_id", 1)])
db.movies.create_index([("start_year", 1)])

db.ratings.create_index([("movie_id", 1)])
db.genres.create_index([("movie_id", 1)])
db.genres.create_index([("genre", 1)])

db.directors.create_index([("movie_id", 1)])
db.directors.create_index([("person_id", 1)])

db.principals.create_index([("movie_id", 1)])
db.principals.create_index([("person_id", 1)])
db.principals.create_index([("person_id", 1), ("category", 1)])

db.characters.create_index([("movie_id", 1), ("person_id", 1)])
db.titles.create_index([("movie_id", 1)])

# ----------------------------
# Reset output collection
# ----------------------------
db[OUT_COL].drop()

# ----------------------------
# Pipeline
# ----------------------------
pipeline = []

# Base = movies
pipeline += [
    {"$project": {
        "_id": "$movie_id",
        "movie_id": "$movie_id",
        "title": "$primary_title",
        "year": "$start_year",
        "runtime": "$runtime_minutes",
    }}
]

# (Optionnel) test sur 1 film
if not TEST_MOVIE_ID and LIMIT_MOVIES:
    pipeline += [{"$limit": LIMIT_MOVIES}]

# genres
pipeline += [
    {"$lookup": {
        "from": "genres",
        "localField": "movie_id",
        "foreignField": "movie_id",
        "pipeline": [{"$project": {"_id": 0, "genre": 1}}],
        "as": "g"
    }},
    {"$set": {
        "genres": {
            "$setUnion": [[], {"$map": {"input": "$g", "as": "x", "in": "$$x.genre"}}]
        }
    }},
]

# rating
pipeline += [
    {"$lookup": {
        "from": "ratings",
        "localField": "movie_id",
        "foreignField": "movie_id",
        "pipeline": [{"$project": {"_id": 0, "average_rating": 1, "num_votes": 1}}],
        "as": "r"
    }},
    {"$set": {
        "rating": {
            "average": {"$ifNull": [{"$first": "$r.average_rating"}, None]},
            "votes": {"$ifNull": [{"$first": "$r.num_votes"}, None]},
        }
    }},
]

# directors: directors -> persons (SANS $getField)
pipeline += [
    {"$lookup": {
        "from": "directors",
        "localField": "movie_id",
        "foreignField": "movie_id",
        "pipeline": [{"$project": {"_id": 0, "person_id": 1}}],
        "as": "dir"
    }},
    {"$lookup": {
        "from": "persons",
        "let": {"ids": "$dir.person_id"},
        "pipeline": [
            {"$match": {"$expr": {"$in": ["$person_id", "$$ids"]}}},
            {"$project": {"_id": 0, "person_id": 1, "name": 1}}
        ],
        "as": "dir_people"
    }},
    {"$set": {
        "directors": {
            "$map": {
                "input": "$dir",
                "as": "d",
                "in": {
                    "person_id": "$$d.person_id",
                    "name": {
                        "$let": {
                            "vars": {
                                "pp": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$dir_people",
                                            "as": "p",
                                            "cond": {"$eq": ["$$p.person_id", "$$d.person_id"]}
                                        }
                                    }
                                }
                            },
                            "in": {"$ifNull": ["$$pp.name", None]}
                        }
                    }
                }
            }
        }
    }},
]

# cast = principals(actor/actress) + persons + characters (SANS $getField)
pipeline += [
    {"$lookup": {
        "from": "principals",
        "localField": "movie_id",
        "foreignField": "movie_id",
        "pipeline": [
            {"$match": {"category": {"$in": ["actor", "actress"]}}},
            {"$project": {"_id": 0, "person_id": 1, "ordering": 1}},
            {"$sort": {"ordering": 1}}
        ],
        "as": "cast_pr"
    }},
    {"$lookup": {
        "from": "persons",
        "let": {"ids": "$cast_pr.person_id"},
        "pipeline": [
            {"$match": {"$expr": {"$in": ["$person_id", "$$ids"]}}},
            {"$project": {"_id": 0, "person_id": 1, "name": 1}}
        ],
        "as": "cast_people"
    }},
    {"$lookup": {
        "from": "characters",
        "let": {"mid": "$movie_id"},
        "pipeline": [
            {"$match": {"$expr": {"$eq": ["$movie_id", "$$mid"]}}},
            {"$group": {"_id": "$person_id", "characters": {"$addToSet": "$name"}}},
            {"$project": {"_id": 0, "person_id": "$_id", "characters": 1}}
        ],
        "as": "cast_chars"
    }},
    {"$set": {
        "cast": {
            "$map": {
                "input": "$cast_pr",
                "as": "pr",
                "in": {
                    "person_id": "$$pr.person_id",
                    "ordering": "$$pr.ordering",
                    "name": {
                        "$let": {
                            "vars": {
                                "pp": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$cast_people",
                                            "as": "p",
                                            "cond": {"$eq": ["$$p.person_id", "$$pr.person_id"]}
                                        }
                                    }
                                }
                            },
                            "in": {"$ifNull": ["$$pp.name", None]}
                        }
                    },
                    "characters": {
                        "$let": {
                            "vars": {
                                "cc": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$cast_chars",
                                            "as": "c",
                                            "cond": {"$eq": ["$$c.person_id", "$$pr.person_id"]}
                                        }
                                    }
                                }
                            },
                            "in": {"$ifNull": ["$$cc.characters", []]}
                        }
                    }
                }
            }
        }
    }},
]

# writers = principals(category == writer) + persons (SANS $getField)
pipeline += [
    {"$lookup": {
        "from": "principals",
        "localField": "movie_id",
        "foreignField": "movie_id",
        "pipeline": [
            {"$match": {"category": "writer"}},
            {"$project": {"_id": 0, "person_id": 1, "job": 1, "ordering": 1}},
            {"$sort": {"ordering": 1}}
        ],
        "as": "w_pr"
    }},
    {"$lookup": {
        "from": "persons",
        "let": {"ids": "$w_pr.person_id"},
        "pipeline": [
            {"$match": {"$expr": {"$in": ["$person_id", "$$ids"]}}},
            {"$project": {"_id": 0, "person_id": 1, "name": 1}}
        ],
        "as": "w_people"
    }},
    {"$set": {
        "writers": {
            "$map": {
                "input": "$w_pr",
                "as": "w",
                "in": {
                    "person_id": "$$w.person_id",
                    "name": {
                        "$let": {
                            "vars": {
                                "pp": {
                                    "$first": {
                                        "$filter": {
                                            "input": "$w_people",
                                            "as": "p",
                                            "cond": {"$eq": ["$$p.person_id", "$$w.person_id"]}
                                        }
                                    }
                                }
                            },
                            "in": {"$ifNull": ["$$pp.name", None]}
                        }
                    },
                    "category": {"$ifNull": ["$$w.job", "writer"]}
                }
            }
        }
    }},
]

# titles (aka)
pipeline += [
    {"$lookup": {
        "from": "titles",
        "localField": "movie_id",
        "foreignField": "movie_id",
        "pipeline": [{"$project": {"_id": 0, "region": 1, "title": 1}}],
        "as": "titles"
    }},
]

# cleanup + merge
pipeline += [
    {"$project": {
        "movie_id": 0,   # déjà dans _id
        "g": 0, "r": 0,
        "dir": 0, "dir_people": 0,
        "cast_pr": 0, "cast_people": 0, "cast_chars": 0,
        "w_pr": 0, "w_people": 0,
    }},
    {"$merge": {"into": OUT_COL, "on": "_id", "whenMatched": "replace", "whenNotMatched": "insert"}}
]

# ----------------------------
# Run
# ----------------------------
db.movies.aggregate(pipeline, allowDiskUse=True)

print("OK. docs movies_complete =", db[OUT_COL].count_documents({}))
print("Example:", db[OUT_COL].find_one())

client.close()
