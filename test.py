from pymongo import MongoClient

uri = "mongodb+srv://tuduong05042003:TCNvGWABP04DAkBZ@natours-app-cluster.us9ca.mongodb.net/"
try:
    client = MongoClient(uri)
    print("Connected successfully!")
    
    db = client["mydatabase"]
    collection = db["mycollection"]

    document = {"name": "Alice", "age": 25, "city": "New York"}
    result = collection.insert_one(document)
    print(f"Inserted document ID: {result.inserted_id}")
except Exception as e:
    print("Error:", e)
finally:
    client.close()
