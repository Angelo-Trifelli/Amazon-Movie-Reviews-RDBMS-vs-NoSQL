-- QUERY 1
-- Extract products liked by users similar to a target user, excluding ones that have been already
-- rated by the target user, and rank them by similarity weight
SELECT
    p.id AS recommended_product,
    SUM(similar_users.similarity_weight) AS strength
FROM (
    SELECT
        r2.reviewer_id,
        COUNT(*) AS similarity_weight
    FROM review r1
    JOIN review r2 ON r1.product_id = r2.product_id AND r1.reviewer_id <> r2.reviewer_id
    WHERE r1.reviewer_id = 'A5KIX8Y8H197J'
    AND r1.score >= 4
    AND r2.score >= 4
    GROUP BY r2.reviewer_id 
) AS similar_users
JOIN review r ON r.reviewer_id = similar_users.reviewer_id
JOIN product p ON p.id = r.product_id
WHERE r.score >= 4
AND NOT EXISTS (
    SELECT 1
    FROM review r0
    WHERE r0.reviewer_id = 'A5KIX8Y8H197J'
    AND r0.product_id = r.product_id
)
GROUP BY p.id
ORDER BY strength DESC;


-- QUERY 2
-- Compute a similarity between all pairs of reviewers based on shared high-rated products
SELECT 
    r1.reviewer_id AS first_reviewer, 
    r2.reviewer_id AS second_reviewer, 
    COUNT(*) AS similarity_score
FROM review r1 
JOIN review r2 ON r1.product_id = r2.product_id AND r1.reviewer_id < r2.reviewer_id
WHERE r1.score >= 4
AND r2.score >= 4
GROUP BY r1.reviewer_id, r2.reviewer_id
HAVING COUNT(*) > 5
ORDER BY similarity_score DESC;

-- QUERY 3
-- Given a target user, find users who highly rated the same products of the target user in a limited time window and 
-- count how many distinct products have been rated in such time window.
SELECT
    r2.reviewer_id AS user,
    COUNT(DISTINCT r1.product_id) AS temporal_overlap
FROM review r1
JOIN review r2 ON r1.product_id = r2.product_id AND r1.reviewer_id <> r2.reviewer_id
WHERE r1.reviewer_id = 'A5KIX8Y8H197J'
AND r1.score >= 4
AND r2.score >= 4
AND ABS(r1.created_at - r2.created_at) < 2592000
GROUP BY r2.reviewer_id
HAVING COUNT(DISTINCT r1.product_id) >= 3
ORDER BY temporal_overlap DESC;