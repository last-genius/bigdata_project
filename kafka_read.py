from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Spark Kafka Streaming") \
        .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-server:9092") \
  .option("subscribe", "changes") \
  .load()

df.printSchema()