from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from utils.config_loader import load_config
from scripts.schema import transaction_schema
from scripts.preprocessing import cast_features_to_double
from scripts.model_inference import predict_fraud


configuration = load_config()

KAFKA_SERVER = configuration["kafka"]["server"]
KAFKA_PORT = configuration["kafka"]["port"]
KAFKA_TOPIC_TRANSACTIONS = configuration["kafka"]["topics"]["transactions"]
KAFKA_TOPIC_FRAUDS = configuration["kafka"]["topics"]["fraud_alerts"]
SPARK_APP_NAME = configuration["spark"]["app_name"]
SPARK_CHECKPOINT_LOCATION = configuration["spark"]["checkpoint_location"]


spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

kafka_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{KAFKA_SERVER}:{KAFKA_PORT}") \
    .option("subscribe", KAFKA_TOPIC_TRANSACTIONS) \
    .load()

# Transform Kafka raw data to structured format
json_df = kafka_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), transaction_schema).alias("data")) \
        .select("data.*")

# Cast features to appropriate types
features = [f"V{i}" for i in range(1, 29)] + ["Amount"]
json_df = cast_features_to_double(json_df, features)

# Apply model inference
predictions_df = json_df.withColumn("is_fraud", predict_fraud(*features))

# Raise an alert: send fraudulent transactions to a Kafka topic
fraudlent_transactions = predictions_df.filter(col("is_fraud") == 1)
fraudlent_transactions.selectExpr("CAST(transaction_id AS STRING) as key",
                                  "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{KAFKA_SERVER}:{KAFKA_PORT}") \
    .option("topic", KAFKA_TOPIC_FRAUDS) \
    .option("checkpointLocation", SPARK_CHECKPOINT_LOCATION) \
    .outputMode("append") \
    .start().awaitTermination()
