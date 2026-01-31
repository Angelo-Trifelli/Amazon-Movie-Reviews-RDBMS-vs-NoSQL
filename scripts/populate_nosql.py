import gzip 
import nosql_utils
from tqdm import tqdm
from collections import defaultdict


DATA_PATH = "data/movies.txt.gz"
BATCH_SIZE = 10000
TOTAL_LINES = 71205231

REVIEWER_FIELDS = ["review/userId", "review/profileName"]
PRODUCT_FIELDS = ["product/productId"]

# Change as needed
POPULATE_PRODUCT_COLLECTION = True
POPULATE_REVIEW_COLLECTION = True

# MongoDB connection
database = nosql_utils.get_db_connection()
products_collection = database["products"]
reviews_collection = database["reviews"]

current_review = {}
current_reviewer = {}
current_product = {}

product_review_buffer = defaultdict(list)

product_bulk_operations = []
review_bulk_operations = []

review_counter = 0
total_invalid_reviews = 0

def flush_bulk():
    if len(product_bulk_operations) > 0:
        products_collection.bulk_write(product_bulk_operations, ordered=False)
        product_review_buffer.clear()
        product_bulk_operations.clear()

    if len(review_bulk_operations) > 0:
        reviews_collection.bulk_write(review_bulk_operations, ordered=False)        
        review_bulk_operations.clear()


with gzip.open(DATA_PATH, "rt", encoding="utf-8", errors="ignore") as file:
    for line in tqdm(file, total=TOTAL_LINES, desc="Scanning file", unit=" lines"):
        line = line.strip()
        colon_index = line.find(":")

        # If true, we have an empty line --> we have collected all the data from the current review and we can persist it
        if colon_index == -1:
            reviewer_id = current_reviewer.get("review/userId")
            product_id = current_product.get("product/productId")
            raw_time = current_review.get("review/time")

            if raw_time is None or reviewer_id is None or product_id is None:
                total_invalid_reviews += 1
                current_review = {}
                current_reviewer = {}
                current_product = {}
                continue

            helpfulness = current_review.get("review/helpfulness", "0/0").split("/")

            product_review_doc = {
                "review_id": review_counter,
                "created_at": int(raw_time),
                "score": float(current_review.get("review/score")),
                "helpfulness": {
                    "num": int(helpfulness[0]),
                    "den": int(helpfulness[1])
                },
                "summary": current_review.get("review/summary"),
                "review_text": current_review.get("review/text"),
                "reviewer": {
                    "id": reviewer_id,
                    "profile_name": current_reviewer.get("review/profileName")
                }
            }

            review_doc = {                
                "created_at": int(raw_time),
                "score": float(current_review.get("review/score")),
                "helpfulness": {
                    "num": int(helpfulness[0]),
                    "den": int(helpfulness[1])
                },
                "summary": current_review.get("review/summary"),
                "review_text": current_review.get("review/text"),
                "reviewer": {
                    "id": reviewer_id,
                    "profile_name": current_reviewer.get("review/profileName")
                },
                "product": {
                    "id": product_id
                }
            }

            if POPULATE_PRODUCT_COLLECTION:
                product_review_buffer[product_id].append(product_review_doc)

            if POPULATE_REVIEW_COLLECTION:
                nosql_utils.add_review_operation(review_bulk_operations, review_counter, review_doc)                

            if sum(len(v) for v in product_review_buffer.values()) >= BATCH_SIZE or len(review_bulk_operations) >= BATCH_SIZE:
                for pid, reviews in product_review_buffer.items():
                    nosql_utils.add_product_operation(product_bulk_operations, pid, reviews)
                
                flush_bulk()
            
            review_counter += 1            

            current_review = {}
            current_reviewer = {}
            current_product = {}
            continue

        field_name = line[:colon_index]
        field_value = line[colon_index + 2:]

        if field_name in REVIEWER_FIELDS:
            current_reviewer[field_name] = field_value
        elif field_name in PRODUCT_FIELDS:
            current_product[field_name] = field_value
        else:
            current_review[field_name] = field_value
    
    if sum(len(v) for v in product_review_buffer.values()) > 0 or len(review_bulk_operations) > 0:
        for pid, reviews in product_review_buffer.items():
            nosql_utils.add_product_operation(product_bulk_operations, pid, reviews)
        
        flush_bulk()

print(f"Total invalid reviews found: {total_invalid_reviews}")
print(f"Total valid reviews inserted: {review_counter}")