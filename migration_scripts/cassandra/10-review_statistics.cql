CREATE TABLE review_statistics(
		category TEXT,
        year_month TEXT,
        avg_stars FLOAT,
        total_reviews INT,
        PRIMARY KEY (year_month, category)
);
