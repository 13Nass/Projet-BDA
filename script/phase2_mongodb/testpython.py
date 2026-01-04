from pymongo import MongoClient

URI = "mongodb://localhost:27017"
client = MongoClient(URI, serverSelectionTimeoutMS=3000)

client.admin.command("ping")
print("OK ping")

db = client["cineexplorer"]
col = db["movies_test"]

col.insert_one({"title": "Inception", "year": 2010})
print("Inserted 1 doc")

for doc in col.find().limit(5):
    print(doc)

client.close()
