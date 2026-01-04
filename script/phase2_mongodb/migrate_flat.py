# -*- coding: utf-8 -*-
"""
Created on Fri Dec 26 14:24:21 2025

@author: bendr
"""

#!/usr/bin/env python3
import argparse
import sqlite3
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

def list_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """)
    return [r[0] for r in cur.fetchall()]

def sqlite_count(conn: sqlite3.Connection, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    return int(cur.fetchone()[0])

def migrate_table(conn: sqlite3.Connection, mongo_db, table: str, batch_size: int, use_pk_as_id: bool):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{table}"')
    colnames = [d[0] for d in cur.description]

    collection = mongo_db[table]

    inserted = 0
    batch = []

    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break

        for row in rows:
            doc = dict(zip(colnames, row))

            # Optionnel: si la table a une colonne "id", on la met en _id pour garder une clé stable
            if use_pk_as_id and "id" in doc and doc["id"] is not None:
                doc["_id"] = doc["id"]

            batch.append(doc)

        try:
            if batch:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
        except BulkWriteError as e:
            # Si doublons sur _id (ou autre), on compte quand même les inserts validés
            inserted += e.details.get("nInserted", 0)
        finally:
            batch = []

    return inserted

def main():
    ap = argparse.ArgumentParser(description="Migrate SQLite tables to MongoDB (flat collections).")
    ap.add_argument("--sqlite", required=True, help="Path to SQLite database file (NOT CSV).")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    ap.add_argument("--mongo-db", default="cineexplorer_flat", help="Target MongoDB database name")
    ap.add_argument("--batch-size", type=int, default=2000, help="Insert batch size")
    ap.add_argument("--drop", action="store_true", help="Drop collections before inserting")
    ap.add_argument("--pk-as-id", action="store_true", help='Use column "id" as MongoDB _id when possible')
    args = ap.parse_args()

    # SQLite
    conn = sqlite3.connect(args.sqlite)
    conn.row_factory = None

    # Mongo
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    mdb = client[args.mongo_db]

    tables = list_tables(conn)
    if not tables:
        print("No tables found in SQLite.")
        return

    print(f"SQLite DB: {args.sqlite}")
    print(f"Mongo URI: {args.mongo_uri}")
    print(f"Mongo DB : {args.mongo_db}")
    print(f"Tables   : {len(tables)}")
    print("-" * 60)

    for t in tables:
        if args.drop:
            mdb[t].drop()

        expected = sqlite_count(conn, t)
        inserted = migrate_table(conn, mdb, t, args.batch_size, args.pk_as_id)
        got = mdb[t].count_documents({})

        status = "OK" if expected == got else "MISMATCH"
        print(f"{t:30s} sqlite={expected:8d} mongo={got:8d} inserted={inserted:8d} => {status}")

    conn.close()
    client.close()

if __name__ == "__main__":
    main()
