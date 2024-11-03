# Start with the official Apache Airflow image
FROM apache/airflow:2.5.0

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow

# Install dependencies
# Install psycopg2-binary for PostgreSQL support, pandas for data manipulation,
# and SQLAlchemy for connecting to the PostgreSQL database.
RUN pip install --no-cache-dir \
    pandas==2.2.2 \
    sqlalchemy==2.0.36 \
    psycopg2-binary==2.9.10 \
    apache-airflow==2.10.2

# Copy DAGs and scripts into the container
COPY dags/ $AIRFLOW_HOME/dags/
COPY scripts/ $AIRFLOW_HOME/scripts/

# Set permissions (optional, ensures that files are accessible)
RUN chmod -R 755 $AIRFLOW_HOME/dags/ && \
    chmod -R 755 $AIRFLOW_HOME/scripts/

# Initialize Airflow (e.g., create necessary folders for logs)
RUN mkdir -p $AIRFLOW_HOME/logs && \
    mkdir -p $AIRFLOW_HOME/plugins

# Expose the default Airflow web server port
EXPOSE 8080

# Default Airflow command to run when container starts
CMD ["airflow", "webserver"]
