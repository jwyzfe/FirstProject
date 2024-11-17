from pymongo import MongoClient

# MongoDB connection settings
ip_add = 'mongodb://192.168.0.48:27017/'  # Replace with your MongoDB URI
db_name = 'DB_SGMN'  # Replace with your database name
col_name = 'COL_FINANCIAL_WORK'  # Replace with your collection name

# Connect to MongoDB
mongo_client = MongoClient(ip_add)
db = mongo_client[db_name]
collection = db[col_name]

try:
    # Delete all documents where iswork is 'fin'
    result = collection.delete_many({'iswork': 'fin'})
    print(f"Deleted {result.deleted_count} documents where iswork is 'fin'.")
except Exception as e:
    print(f"Error occurred: {str(e)}")
finally:
    mongo_client.close()