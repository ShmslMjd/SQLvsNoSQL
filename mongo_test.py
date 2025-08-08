from pymongo import MongoClient
import pprint
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connection string from environment variable:
uri = os.getenv("MONGODB_URI")

# Connect to MongoDB Atlas
client = MongoClient(uri)

# Access the database and collection
db = client["testdb"]
collection = db["users"]

# Sample data to insert
sample_user = {
    "name": "Alice",
    "email": "alice@example.com",
    "age": 25,
    "interests": ["reading", "coding", "travel"]
}

# Insert the document
insert_result = collection.insert_one(sample_user)
print(f"Inserted document ID: {insert_result.inserted_id}")

# Fetch and display the document just inserted
result = collection.find_one({"_id": insert_result.inserted_id})
print("Fetched document:")
pprint.pprint(result)