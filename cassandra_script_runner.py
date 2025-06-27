from itertools import islice, tee
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DCAwareRoundRobinPolicy
from cassandra.pool import HostDistance
from cassandra import ConsistencyLevel
import logging
import time
from pyspark.sql import SparkSession, Row

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("my_custom_logger")


def execute_query(
    session, query_number: int, query: str, table: str, spark: SparkSession = None
):
    logger.info(f"Executing query {query_number}...")
    start = time.perf_counter()
    rows = session.execute(query, timeout=60)
    end = time.perf_counter()
    logger.info(f"Execution time: {end - start:.4f} seconds")

    preview_iter, count_iter = tee(rows)

    logger.info("🔹 Previewing first 5 rows:")
    for i, row in enumerate(islice(rows, 5), 1):
        logger.info(f"   Row {i}: {row}")

    total_rows = sum(1 for _ in count_iter)
    logger.info(f"🔸 Total rows: {total_rows+ 5}")


if __name__ == "__main__":
    logger.info("Starting Cassandra script runner...")
    logger.info("Connecting to Cassandra cluster...")

    execution_profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    )

    cluster = Cluster(
        contact_points=["cassandra", "cassandra2"],
        protocol_version=5,
        execution_profiles={EXEC_PROFILE_DEFAULT: execution_profile},
    )

    session = cluster.connect("yelp")
    logger.info("Successfully Connected to Cassandra cluster.")
    logger.info("Executing queries...")

    query = """
        SELECT review_id,business_id, stars, city 
        from three_star_reviews
        where stars = 3 and city = 'New Orleans'
        ALLOW FILTERING;
    """
    table = "three_star_reviews"
    execute_query(session, 1, query, table)

    query = """
        SELECT business_id, review_id, stars, date from reviews_by_year
        where stars = 5 and date >= '2022-01-01' AND date < '2023-01-01'    
        ALLOW FILTERING;
    """
    table = "reviews_by_year"
    execute_query(session, 2, query, table)

    query = """
        SELECT user_id, yelping_since,review_count from users_with_reviews_yelping_since
        where yelping_since >= '2017-01-01' AND review_count > 500
        ALLOW FILTERING;
    """
    table = "users_with_reviews_yelping_since"
    execute_query(session, 3, query, table)

    query = """
        SELECT review_count, city from business_reviews_per_city;
    """
    table = "business_reviews_per_city"
    execute_query(session, 4, query, table)

    query = """
        SELECT business_id,average_word_count_on_review from average_words_on_review_for_a_business;
    """
    table = "average_words_on_review_for_a_business"
    execute_query(session, 5, query, table)

    query = """
        SELECT category, business_count, average_stars from business_count_per_category_and_average_star_review;
    """
    table = "business_count_per_category_and_average_star_review"
    execute_query(session, 6, query, table)

    query = """
        SELECT stars,number_of_reviews from reviews_by_star;
    """
    table = "reviews_by_star"
    execute_query(session, 7, query, table)

    query = """
        SELECT city,category, average_rating from top_categoreies_by_city;
    """
    table = "top_categoreies_by_city"
    execute_query(session, 8, query, table)

    query = """
        SELECT user_id,review_count, number_of_reviewed_categories, number_of_useful_reviews from user_statistics;
    """
    table = "user_statistics"
    execute_query(session, 9, query, table)

    query = """
        SELECT category, year_month, avg_stars,total_reviews from review_statistics
        where year_month >= '2021-01' AND year_month < '2022-01'
        ALLOW FILTERING;
    """
    table = "review_statistics"
    execute_query(session, 10, query, table)
