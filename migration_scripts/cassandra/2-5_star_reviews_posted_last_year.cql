CREATE TABLE reviews_by_year(
    business_id TEXT,
    review_id TEXT,
    stars INT,
    date DATE,
    PRIMARY KEY ((business_id, review_id), date, stars)
);
