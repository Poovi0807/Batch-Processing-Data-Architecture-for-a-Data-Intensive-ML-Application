FROM apache/airflow:3.3.2-python3.12

# Spark Connect client (pure Python, no Java needed). The constraints file of the
# Airflow release keeps every other package at the version Airflow was tested with.
RUN pip install --no-cache-dir \
      "apache-airflow==3.3.2" \
      "pyspark-client==4.2.0" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"
