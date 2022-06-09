from pyspark.sql import SparkSession
from pyspark.sql.streaming import *
from pyspark.sql.functions import *

spark = SparkSession \
        .builder \
        .appName("Spark Kafka Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
        .getOrCreate()


kafka_bootstrap_servers = "kafka-server:9092"
input_topic_name = "demo-topic"
output_topic_name = "receiver-topic"


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()


df.select(upper(col("value")).alias("value"))\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", output_topic_name) \
    .option("checkpointLocation", "/opt/app/kafka_checkpoint").start().awaitTermination()
