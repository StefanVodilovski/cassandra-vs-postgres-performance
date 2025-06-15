CREATE TABLE yelp.three_star_reviews(
	business_id TEXT,
	review_id TEXT,
    stars INT,  
    city TEXT,
    PRIMARY KEY ((business_id, review_id), city, stars)
);
