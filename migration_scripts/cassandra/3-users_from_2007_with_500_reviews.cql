CREATE TABLE users_with_reviews_yelping_since(
    user_id TEXT,
    review_id TEXT,
    review_count INT,
    yelping_since TIMESTAMP,
    PRIMARY KEY ((user_id, review_id), yelping_since , review_count)
);
