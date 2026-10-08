# Official Apache Spark image (Docker Official Image, with Python 3).
# Additions: the PostgreSQL JDBC driver, the job gateway and the job scripts.
FROM spark:4.2.0-python3

USER root

# JDBC driver used to load the feature table into PostgreSQL
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/

RUN mkdir -p /opt/spark/work /opt/spark/logs /var/spark-jobs \
 && chown -R spark:spark /opt/spark/work /opt/spark/logs /var/spark-jobs

COPY gateway/ /opt/pipeline/gateway/
COPY jobs/    /opt/pipeline/jobs/
COPY tests/   /opt/pipeline/tests/

USER spark
WORKDIR /opt/pipeline
