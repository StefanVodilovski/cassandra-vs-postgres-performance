CREATE TABLE business_reviews_per_city(
    review_count INT,
    city TEXT,
    PRIMARY KEY ((city, review_count))
);
