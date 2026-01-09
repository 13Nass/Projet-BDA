# -*- coding: utf-8 -*-
"""
MongoDB service layer for CineExplorer.

Attendus dans settings.py :
- MONGO_URI
- MONGO_DB_NAME (ou legacy MONGO_DB)

Collections utilisées (typique) :
- movies_complete
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Optional

from django.conf import settings
from pymongo import MongoClient


def _get_db_name() -> str:
    name = getattr(settings, "MONGO_DB_NAME", None) or getattr(settings, "MONGO_DB", None)
    if not name:
        raise AttributeError("settings.MONGO_DB_NAME (ou settings.MONGO_DB) manquant")
    return str(name)


def _get_uri() -> str:
    return getattr(settings, "MONGO_URI", None) or "mongodb://127.0.0.1:27017"


@lru_cache(maxsize=1)
def _client() -> MongoClient:
    # timeouts un peu larges pour éviter les faux timeouts
    return MongoClient(
        _get_uri(),
        serverSelectionTimeoutMS=20000,
        connectTimeoutMS=20000,
        socketTimeoutMS=20000,
    )


def db():
    return _client()[_get_db_name()]


def hello_info() -> dict[str, Any]:
    # juste une info simple (utile debug/rapport)
    return {"db": _get_db_name(), "uri": _get_uri()}


def all_collection_counts(max_collections: int = 12) -> dict[str, int]:
    d = db()
    names = sorted(d.list_collection_names())
    out: dict[str, int] = {}
    for name in names[:max_collections]:
        try:
            out[name] = int(d[name].estimated_document_count())
        except Exception:
            out[name] = 0
    return out


@lru_cache(maxsize=1)
def _cached_movie_ids() -> list[str]:
    d = db()
    if "movies_complete" not in d.list_collection_names():
        return []
    # on ne récup que _id
    cur = d["movies_complete"].find({}, {"_id": 1})
    return [doc["_id"] for doc in cur]


def movie_ids_if_small(max_docs: int = 5000) -> Optional[list[str]]:
    """
    Si la collection movies_complete est "petite", on renvoie la liste des ids
    pour filtrer la liste SQLite et éviter les 404 sur le détail.
    Sinon -> None (pas de filtre).
    """
    d = db()
    if "movies_complete" not in d.list_collection_names():
        return None
    n = int(d["movies_complete"].estimated_document_count())
    if n <= int(max_docs):
        return _cached_movie_ids()
    return None


def get_movie_complete(movie_id: str) -> Optional[dict[str, Any]]:
    d = db()
    if "movies_complete" not in d.list_collection_names():
        return None
    return d["movies_complete"].find_one({"_id": str(movie_id)})
