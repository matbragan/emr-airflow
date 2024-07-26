FROM apache/airflow:2.9.0

# Copy shell script to initialize airflow, with connections
USER root
COPY init_airflow.sh /init_airflow.sh
RUN chmod +x /init_airflow.sh

# Install python dependencies
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
