# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from pyspark.sql import SparkSession
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'


# додати в databricks кластер вручну бібліотеку (maven):
# org.apache.kafka:kafka-clients:3.2.3

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "jane_building_sensors") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")


schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])


from pyspark.sql.functions import from_json

df_parsed = df.withColumn("data", from_json(col("value"), schema)).select("data.*")

display(df_parsed)



# COMMAND ----------

from pyspark.sql.functions import window, avg

from pyspark.sql.functions import col, from_unixtime

df_parsed = df.withColumn("data", from_json(col("value"), schema)).select("data.*") \
              .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

agg_df = (df_parsed
    .withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
    .agg(avg("temperature").alias("t_avg"), avg("humidity").alias("h_avg"))
)

display(agg_df)

# COMMAND ----------

alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", DoubleType(), True),
    StructField("humidity_max", DoubleType(), True),
    StructField("temperature_min", DoubleType(), True),
    StructField("temperature_max", DoubleType(), True),
    StructField("code", StringType(), True),
    StructField("message", StringType(), True)
])

alerts_df = (spark.read
    .format("csv")
    .option("header", True)
    .schema(alerts_schema)
    .load("dbfs:/FileStore/shared_uploads/alerts_conditions/")
)

display(alerts_df)

# COMMAND ----------

from pyspark.sql.functions import expr

joined_df = agg_df.crossJoin(alerts_df)

# display(joined_df)

alert_result = (joined_df
    .filter(
        (
            # Перевірка для температури:
            ((col("temperature_min") == -999) | (col("t_avg") >= col("temperature_min"))) &
            ((col("temperature_max") == -999) | (col("t_avg") <= col("temperature_max")))
        )
        &
        (
            # Перевірка для вологості:
            ((col("humidity_min") == -999) | (col("h_avg") >= col("humidity_min"))) &
            ((col("humidity_max") == -999) | (col("h_avg") <= col("humidity_max")))
        )
    )
    .selectExpr(
        "window.start as window_start", "window.end as window_end", "t_avg", "h_avg", "code", "message"
    )
)


display(alert_result)


# COMMAND ----------

from pyspark.sql.functions import to_json, struct, current_timestamp

output_topic = "jane_alert_kafka_topic_spark_streaming"

final_df = alert_result.select(
    to_json(struct(
        struct("window_start", "window_end").alias("window"),
        "t_avg", "h_avg", "code", "message",
        current_timestamp().alias("timestamp")
    )).alias("value")
)

display(final_df)

(final_df.writeStream
    .trigger(processingTime='5 seconds')
    .format("kafka")
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') 
    .option("topic", output_topic)
    .option("checkpointLocation", "/mnt/path/checkpoints/")
    .start()
    .awaitTermination()
)


