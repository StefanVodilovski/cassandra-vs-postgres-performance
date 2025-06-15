CREATE TABLE user_statistics(
		user_id TEXT,
        review_count INT,
        number_of_reviewed_categories INT,
        number_of_useful_reviews INT,
        PRIMARY KEY (number_of_reviewed_categories, user_id)
);
