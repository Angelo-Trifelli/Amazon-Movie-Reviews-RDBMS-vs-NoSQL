from pymongo import MongoClient, UpdateOne


def get_db_connection():
    return MongoClient(
        host="localhost",
        port=27017,
        username="admin",
        password="admin",
        authSource="admin"
    )["amazon-movie-reviews"]


def add_product_operation(bulk_operations, product_id, reviews):
    bulk_operations.append(
        UpdateOne(
            {"_id": product_id},
            {"$push": {"reviews": {"$each": reviews}}},
            upsert=True
        )
    )

def add_review_operation(bulk_operations, review_id, review):
    bulk_operations.append(
        UpdateOne(
            {"_id": review_id},
            {"$set": review},
            upsert=True
        )
    )