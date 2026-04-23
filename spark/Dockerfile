FROM public.ecr.aws/bitnami/spark:3.5.3

USER root

RUN mkdir -p /opt/bitnami/spark/custom-jars \
    && chown 1001:1001 /opt/bitnami/spark/custom-jars

# PostgreSQL JDBC driver
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/bitnami/spark/custom-jars/

# ClickHouse JDBC driver (official, com.clickhouse.jdbc.ClickHouseDriver, fat jar with dependencies)
ADD https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar /opt/bitnami/spark/custom-jars/

RUN chown 1001:1001 /opt/bitnami/spark/custom-jars/*.jar

RUN pip install --no-cache-dir pyspark==3.5.3

# Добавляем запись для uid 1001 — иначе Hadoop/JVM падают с "invalid null input: name"
RUN echo "spark:x:1001:1001:spark:/tmp:/bin/bash" >> /etc/passwd \
    && echo "spark:x:1001:" >> /etc/group

USER 1001

ENV SPARK_EXTRA_JARS=/opt/bitnami/spark/custom-jars/postgresql-42.7.3.jar,/opt/bitnami/spark/custom-jars/clickhouse-jdbc-0.6.3-all.jar
