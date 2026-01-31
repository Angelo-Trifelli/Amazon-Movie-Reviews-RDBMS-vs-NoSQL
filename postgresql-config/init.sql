CREATE TABLE IF NOT EXISTS reviewer (
    id VARCHAR(255) PRIMARY KEY,
    profile_name VARCHAR(255) NOT NULL
);


CREATE TABLE IF NOT EXISTS product (
    id VARCHAR(255) PRIMARY KEY
);


CREATE TABLE IF NOT EXISTS review (
    id BIGSERIAL PRIMARY KEY,
    created_at BIGINT NOT NULL,
    score NUMERIC(2, 1) NOT NULL,
    helpfulness_numerator INTEGER,
    helpfulness_denominator INTEGER,
    summary TEXT,
    review_text TEXT,
    reviewer_id VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    CONSTRAINT FK_Reviewer_Id FOREIGN KEY (reviewer_id) REFERENCES reviewer(id),
    CONSTRAINT FK_Product_Id FOREIGN KEY (product_id) REFERENCES product(id)
);

