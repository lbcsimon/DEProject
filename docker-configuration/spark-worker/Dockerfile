FROM spark-base

EXPOSE 8081

CMD spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> spark/logs/spark-worker.out
