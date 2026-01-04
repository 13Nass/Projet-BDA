# movies/services/mongo_service.py
from __future__ import annotations

from typing import Any, Dict, Optional
from django.conf import settings
from pymongo import MongoClient

_client: Optional[MongoClient] = None


def _mongo_uri() -> str:
    uri = getattr(settings, "MONGO_URI", None)
    if not uri:
        raise RuntimeError("MONGO_URI not configured in settings.py")
    return str(uri)


def _mongo_db_name() -> str:
    # Compat : certains mettent MONGO_DB_NAME, d'autres MONGO_DB
    return str(getattr(settings, "MONGO_DB_NAME", None) or getattr(settings, "MONGO_DB", None) or "cineexplorer_flat")


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(_mongo_uri(), serverSelectionTimeoutMS=3000)
    return _client


def get_db():
    return get_client()[_mongo_db_name()]


def list_collections():
    return get_db().list_collection_names()


def collection_count(coll_name: str) -> int:
    return get_db()[coll_name].count_documents({})


def all_collection_counts(limit: int = 20) -> Dict[str, int]:
    colls = list_collections()[:limit]
    return {c: collection_count(c) for c in colls}


def hello_info() -> Dict[str, Any]:
    # utile pour montrer primary/replica dans le rapport
    return get_client().admin.command("hello")


def get_movie_complete(movie_id: str, collection: str = "movies_complete") -> Optional[Dict[str, Any]]:
    return get_db()[collection].find_one({"_id": movie_id})
