# 📊 Cassandra vs PostgreSQL Performance

A comparative analysis of query performance and use cases between a **NoSQL database (Cassandra)** and a **Relational database (PostgreSQL)**.

We compare query execution times from:
- `postgres-queries.txt`
- `cassandra-queries.txt`

---

## ⚙️ How to Recreate the Experiment

Follow these steps to set up and reproduce the performance test:

1. **Download the Yelp Dataset**  
   📥 [Yelp Dataset on Kaggle](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset)

2. **Install Apache Spark**

3. **Create a Cassandra DB Instance**  
   Use the `docker-compose.yml` file and ensure ports are correctly specified. Follow the offical documentation from the Apache Cassandra website if you are having trouble

4. **Create a PostgreSQL DB Instance**  
   Also available via the same `docker-compose.yml` file.

5. **Create Cassandra Tables**  
   Run the scripts from: [`migration_scripts/cassandra`](migration_scripts/cassandra)

6. **Create PostgreSQL Tables**  
   Run the scripts from: [`migration_scripts/postgres`](migration_scripts/postgres)

7. **Ingest Data Using Spark**  
   Load the Yelp dataset into both Cassandra and PostgreSQL.

8. **Run Benchmark Queries**  
   Execute queries from:
   - `postgres-queries.txt`
   - `cassandra-queries.txt`

---

🧪 This setup allows for a side-by-side comparison of how each database handles large-scale data and different query patterns.
