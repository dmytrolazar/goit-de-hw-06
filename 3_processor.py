from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql.functions import from_json, window, avg, col, from_unixtime, to_json, struct, current_timestamp
from pyspark.sql import SparkSession
from configs import kafka_config, my_name
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

topic_name = f'{my_name}_sensors'

# Читання потоку даних із Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("subscribe", topic_name) \
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

df_parsed = df.withColumn("data", from_json(col("value"), schema)).select("data.*") \
              .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

# Агрегація даних
agg_df = (df_parsed
    .withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
    .agg(avg("temperature").alias("t_avg"), avg("humidity").alias("h_avg"))
)

alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", DoubleType(), True),
    StructField("humidity_max", DoubleType(), True),
    StructField("temperature_min", DoubleType(), True),
    StructField("temperature_max", DoubleType(), True),
    StructField("code", StringType(), True),
    StructField("message", StringType(), True)
])

# Читання параметрів алертів
alerts_df = (spark.read
    .format("csv")
    .option("header", True)
    .schema(alerts_schema)
    .load(".\\alerts_conditions.csv")
)
alerts_df.show()

# Перевірка критеріїв
joined_df = agg_df.crossJoin(alerts_df)

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
    .selectExpr("window.start as window_start", "window.end as window_end", "t_avg", "h_avg", "code", "message")
)

output_topic = f"{my_name}_alerts"

final_df = alert_result.select(
    to_json(struct(
        struct("window_start", "window_end").alias("window"),
        "t_avg", "h_avg", "code", "message",
        current_timestamp().alias("timestamp")
    )).alias("value")
)

# Запис у вихідний топік
(final_df.writeStream
    .trigger(processingTime='5 seconds')
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
    .option("kafka.security.protocol", kafka_config['security_protocol'])
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
    .option("kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
    .option("topic", output_topic)
    .option("checkpointLocation", "/tmp/checkpoints")
    .start()
    .awaitTermination()
)
