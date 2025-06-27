from sqlalchemy import create_engine, text
import logging
import time


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("my_custom_logger")


def execute_query(conn, query_number: int, query: str):
    logger.info(f"Executing query {query_number}...")

    start = time.perf_counter()
    result = conn.execute(text(query))
    rows = result.fetchall()
    end = time.perf_counter()

    logger.info(f"Execution time: {end - start:.4f} seconds")
    logger.info(f"Rows returned: {len(rows)}")

    preview = rows[:5]
    for i, row in enumerate(preview, 1):
        logger.info(f"🔹 Row {i}: {row}")


engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost/postgres")
with engine.connect() as conn:
    query = """SELECT r.review_id, b.business_id, r.stars, b.city
                FROM review r
                JOIN business b ON r.business_id = b.business_id
                WHERE r.stars = 3 AND b.city = 'New Orleans';
            """
    execute_query(conn, 1, query)

    query = """SELECT r.review_id, b.business_id, r.stars, r.date
            FROM business b
            JOIN review r ON b.business_id = r.business_id
            WHERE r.stars = 5 AND EXTRACT(YEAR FROM r.date) = 2022;
            """
    execute_query(conn, 2, query)

    query = """SELECT u.name, u.yelping_since, COUNT(r.review_id) as review_count
                FROM "user"  as u
                JOIN review as r on u.user_id = r.user_id
                WHERE EXTRACT(YEAR FROM u.yelping_since)  >= 2007
                GROUP BY u.user_id, u.yelping_since
                HAVING COUNT(r.review_id) > 500;
            """
    execute_query(conn, 3, query)

    query = """SELECT b.city, COUNT(r.review_id) as review_count
                FROM  business as b
                JOIN review as r ON b.business_id = r.business_id
                GROUP BY b.city
                ORDER BY COUNT(r.review_id) desc;
            """
    execute_query(conn, 4, query)

    query = """
        with review_word_counts  as(
            SELECT business_id,  array_length(string_to_array(text, ' '), 1) AS word_count
            FROM review
        )

        SELECT b.business_id, AVG(word_count) AS average_word_count_on_review
        FROM review_word_counts
        JOIN business b ON b.business_id = review_word_counts.business_id
        GROUP BY b.business_id;
            """
    execute_query(conn, 5, query)

    query = """
        select
            TRIM(category) as category,
            COUNT(*) as business_count,
            ROUND(AVG(stars),2) as average_stars
        from
        (
            select
    	    business_id,
    	    stars,
    	    unnest(STRING_TO_ARRAY(categories,',')) as category
            from business
            where categories is not null) as category_data
        group by TRIM(category) order by business_count desc;
            """
    execute_query(conn, 6, query)

    query = """	SELECT r.stars, COUNT(r.review_id)
                FROM review r
                GROUP BY r.stars;
            """
    execute_query(conn, 7, query)

    query = """CREATE OR REPLACE FUNCTION split_categories(cat_text TEXT)
                RETURNS TABLE(category TEXT) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT TRIM(unnested)
                    FROM UNNEST(STRING_TO_ARRAY(cat_text, ',')) AS unnested;
                END;
                $$ LANGUAGE plpgsql;

                WITH category_expanded AS (
                    SELECT b.city, c.category, b.stars
                    FROM business b,
                        split_categories(b.categories) AS c
                    WHERE b.categories IS NOT NULL
                ),
                category_avg AS (
                    SELECT
                        city,
                        category,
                        ROUND(AVG(stars), 2) AS avg_stars
                    FROM category_expanded
                    GROUP BY city, category
                ),
                ranked_categories AS (
                    SELECT *,
                        RANK() OVER (PARTITION BY city ORDER BY avg_stars DESC) AS category_rank
                    FROM category_avg
                )
                SELECT
                    city,
                    category,
                    avg_stars
                FROM ranked_categories
                WHERE category_rank <= 3
                ORDER BY city, category_rank;
            """
    execute_query(conn, 8, query)

    query = """
            WITH business_category AS (
                SELECT
                    business_id,
                    category
                FROM business,
                    split_categories(categories)
            )
            SELECT
                u.user_id,
                u.name,
                COUNT(DISTINCT r.business_id) AS reviewed_businesses,
                COUNT(DISTINCT bc.category) AS unique_categories_reviewed,
                ROUND(AVG(r.useful), 2) AS avg_useful_votes
            FROM "user" u
            JOIN review r ON u.user_id = r.user_id
            JOIN business_category bc ON r.business_id = bc.business_id
            GROUP BY u.user_id, u.name
            ORDER BY reviewed_businesses DESC;
    """
    execute_query(conn, 9, query)

    query = """
    CREATE OR REPLACE FUNCTION split_categories(cat_text TEXT)
                RETURNS TABLE(category TEXT) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT TRIM(unnested)
                    FROM UNNEST(STRING_TO_ARRAY(cat_text, ',')) AS unnested;
                END;
                $$ LANGUAGE plpgsql;
    
    WITH business_category AS (
    SELECT 
        b.business_id,
        c.category
    FROM business b,
         split_categories(b.categories) AS c
)
SELECT 
    bc.category,
    EXTRACT(MONTH FROM r."date") AS month_in_year,
    COUNT(r.review_id) AS total_reviews,
    ROUND(AVG(r.stars), 2) AS avg_stars
FROM review r
JOIN business_category bc ON r.business_id = bc.business_id
WHERE EXTRACT(YEAR FROM r."date") = 2024
GROUP BY bc.category, month_in_year
ORDER BY bc.category, month_in_year DESC;

            """
    execute_query(conn, 10, query)
