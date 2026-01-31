import gzip
import rdbms_utils
from tqdm import tqdm

DATA_PATH = "data/movies.txt.gz"
BATCH_SIZE = 50000
TOTAL_LINES = 71205231

REVIEWER_FIELDS = ["review/userId", "review/profileName"]
PRODUCT_FIELDS = ["product/productId"]

# Db connection and sql queries
db_connection = rdbms_utils.get_db_connection()
insert_reviewer_query = rdbms_utils.get_insert_reviewer_query()
insert_product_query = rdbms_utils.get_insert_product_query()
insert_review_query = rdbms_utils.get_insert_review_query()

# Data structures to track data for each entity
current_review = {}
current_reviewer = {}
current_product = {}

reviewers_to_insert = []
products_to_insert = []
reviews_to_insert = []

total_invalid_reviews = 0


def insert_and_commit():
    cursor.executemany(insert_reviewer_query, reviewers_to_insert)
    cursor.executemany(insert_product_query, products_to_insert)
    cursor.executemany(insert_review_query, reviews_to_insert)

    db_connection.commit()

    reviewers_to_insert.clear()
    products_to_insert.clear()
    reviews_to_insert.clear()


with db_connection.cursor() as cursor:
    with gzip.open(DATA_PATH, "rt", encoding="utf-8", errors="ignore") as file:
        for line in tqdm(file, total=TOTAL_LINES, desc="Scanning file", unit=" lines"):
            line = line.strip()
            colonIndex = line.find(":")

            # If true, we have an empty line --> we have collected all the data from the current review and we can persist it
            if (colonIndex == -1):
                reviewer_id = current_reviewer.get("review/userId")
                product_id = current_product.get("product/productId")
                
                raw_time = current_review.get("review/time")

                #Skip invalid review
                if raw_time is None or reviewer_id is None:
                    total_invalid_reviews += 1
                    current_reviewer = {}
                    current_product = {}
                    current_review = {}
                    continue

                #Store reviews, reviewers and products information (the db insertion will be performed in batches)
                reviewers_to_insert.append((reviewer_id, current_reviewer.get("review/profileName")))

                products_to_insert.append((product_id, ))
            
                review_helpfulness_split = current_review.get("review/helpfulness", "0/0").split("/")                            

                reviews_to_insert.append((
                    int(raw_time),
                    float(current_review.get("review/score")),
                    int(review_helpfulness_split[0]),
                    int(review_helpfulness_split[1]),
                    current_review.get("review/summary"),
                    current_review.get("review/text"),
                    reviewer_id,
                    product_id
                ))

                if len(reviews_to_insert) >= BATCH_SIZE:
                    insert_and_commit()                   

                current_reviewer = {}
                current_product = {}
                current_review = {}
                continue

            fieldName = line[:colonIndex]
            fieldValue = line[colonIndex + 2:]

            if fieldName in REVIEWER_FIELDS:
                current_reviewer[fieldName] = fieldValue
            elif fieldName in PRODUCT_FIELDS:
                current_product[fieldName] = fieldValue
            else:
                current_review[fieldName] = fieldValue

        #Check if there are some reviews that need to be inserted in the db
        if len(reviews_to_insert) > 0:
            insert_and_commit()
    
    print(f"Total invalid reviews found: {total_invalid_reviews}")




