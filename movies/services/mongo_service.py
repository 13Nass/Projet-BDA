# -*- coding: utf-8 -*-
"""
Created on Tue Dec 30 16:23:25 2025

@author: bendr
"""

from django.conf import settings
from pymongo import MongoClient

_client = None

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(settings.MONGO_URI, serverSelectionTimeoutMS=3000)
    return _client

def get_db():
    return get_client()[settings.MONGO_DB_NAME]

def list_collections():
    return get_db().list_collection_names()

def collection_count(coll_name: str) -> int:
    return get_db()[coll_name].count_documents({})

def all_collection_counts(limit: int = 20) -> dict:
    colls = list_collections()[:limit]
    return {c: collection_count(c) for c in colls}

def hello_info():
    # utile pour rapport : prouver primary/replica
    return get_client().admin.command("hello")
