#!/bin/bash
set -e

JARS="/opt/bitnami/spark/custom-jars/postgresql-42.7.3.jar,/opt/bitnami/spark/custom-jars/clickhouse-jdbc-0.6.3-all.jar"

echo "Ждём запуска PostgreSQL и ClickHouse..."
sleep 30

echo "============================================"
echo "ETL 1: mock_data -> звезда в PostgreSQL"
echo "============================================"
spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --jars "$JARS" /opt/spark/apps/etl_to_star.py

echo ""
echo "============================================"
echo "ETL 2: звезда -> 6 витрин в ClickHouse"
echo "============================================"
spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --jars "$JARS" /opt/spark/apps/etl_to_marts.py

echo ""
echo "ETL завершён."
tail -f /dev/null
