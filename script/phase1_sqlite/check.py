import sqlite3
import pandas as pd
import import_data 

conn = sqlite3.connect("C:/Users/bendr/OneDrive/Documents/Polytech/S7/BDA/data/imdb.db")
print(pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn))
print(pd.read_sql("SELECT COUNT(*) AS n FROM movies;", conn))
conn.close()