# Visable Data Engineer Case Study

This project sets up an ETL pipeline to load, transform, and store data in PostgreSQL for analysis, with deduplication and UPSERT logic.

## Project Structure
- **dags**: Airflow DAGs for scheduling tasks
- **scripts**: Python scripts for ETL (extract, transform, and load) with deduplication and incremental loading
- **sql**: SQL files for creating tables and analysis queries

## Setup Instructions
1. Clone this repository.
2. Update the path of .csv files in extract.py
2. Run `docker-compose up -d` to start PostgreSQL and Airflow.
3. Access the Airflow UI at `http://localhost:8080`.
4. Place CSV files in the data directory with the naming format `yyyymmddhhmmss_TABLENAME.csv`.
5. Trigger the DAG to process and load data.

## Running Analysis Queries
1. Connect to PostgreSQL: `docker exec -it <postgres-container> psql -U dbnsaha -d postgres`.
2. Run queries in `sql/analysis_queries.sql` to get results.
