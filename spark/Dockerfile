# Official Apache Spark image + PostgreSQL driver + our jobs and the gateway.
# The same image is used for the master, the two workers and the gateway.
FROM spark:4.2.0-python3

USER root
# driver that lets Spark write into PostgreSQL
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/
COPY spark-defaults.conf /opt/spark/conf/
COPY jobs/ /opt/jobs/
COPY tests/ /opt/tests/
COPY gateway.py /opt/gateway.py
RUN mkdir -p /opt/spark/work && chown -R spark /opt/spark/work /opt/jobs

USER spark
WORKDIR /opt/jobs
