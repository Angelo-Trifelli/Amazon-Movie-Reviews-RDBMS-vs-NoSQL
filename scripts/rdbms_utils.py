import psycopg


def get_db_connection():
    return psycopg.connect(
        host="localhost",
        port=5432,
        dbname="amazon-movie-reviews",
        user="admin",
        password="admin",
        autocommit=False
    )

def get_insert_reviewer_query():
    return """
        INSERT INTO reviewer (id, profile_name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING;
    """

def get_insert_product_query():
    return """
        INSERT INTO product (id)
        VALUES (%s)
        ON CONFLICT (id) DO NOTHING;
    """

def get_insert_review_query():
    return """
        INSERT INTO review (created_at, score, helpfulness_numerator, helpfulness_denominator, summary, review_text, reviewer_id, product_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """