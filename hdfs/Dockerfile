# Official Apache Hadoop image. The only change: a data directory owned by the
# "hadoop" user, so that the Docker volumes mounted there are writable.
FROM apache/hadoop:3.4.3

USER root
RUN mkdir -p /hadoop/dfs && chown -R hadoop /hadoop
USER hadoop
