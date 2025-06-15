CREATE TABLE business_reviews_by_city(
    business_id  TEXT,
    review_id TEXT,
    city TEXT,
    PRIMARY KEY ((business_id, review_id), city)
);
