from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("RedditKafkaConsumer") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Kafka configurations
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "reddit_sentiment"

# Define schema for incoming Kafka messages
schema = StructType([
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("url", StringType(), True)
])

# Read streaming data from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")  # Start consuming from latest messages
    .load()
)

# Parse JSON messages from Kafka
parsed_stream = (
    kafka_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Define Hive table name
HIVE_TABLE = "default.reddit_sentiment_data"

# Write the parsed data to Hive in Parquet format
query = (
    parsed_stream.writeStream
    .format("parquet")
    .option("checkpointLocation", "hdfs://127.0.0.1:9000/tmp/checkpoints/reddit_sentiment")
    .option("path", "hdfs://127.0.0.1:9000/user/hive/warehouse/reddit_sentiment_data")
    .trigger(processingTime="60 seconds")  # Trigger processing every 60 seconds
    .start()
)

query.awaitTermination()
