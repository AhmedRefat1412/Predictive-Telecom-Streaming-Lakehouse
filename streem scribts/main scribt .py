from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

# =========================================
# 1️⃣ Spark Session
# =========================================
spark = SparkSession.builder \
    .appName("TelecomStreamingUnified") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================
# 2️⃣ Document Schema
# =========================================
tower_schema = MapType(StringType(), StringType())

document_schema = StructType([
    StructField("event_time", StringType()),
    StructField("towers", ArrayType(tower_schema))
])

# =========================================
# 3️⃣ Function to Read and Explode Topic
# =========================================
def read_and_explode(topic_name):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), document_schema).alias("data"))

    exploded_df = parsed_df.select(
        to_timestamp(col("data.event_time")).alias("event_time"),
        explode(col("data.towers")).alias("tower")
    ).select(
        col("event_time"),
        col("tower.tower_id").alias("tower_id"),
        col("tower.region").alias("region"),
        col("tower.*")
    )

    return exploded_df

# =========================================
# 4️⃣ Read and Explode All 4 Streams
# =========================================
system_df  = read_and_explode("telecom.tower.system")
radio_df   = read_and_explode("telecom.tower.radio")
env_df     = read_and_explode("telecom.tower.environment")
network_df = read_and_explode("telecom.tower.network")

# =========================================
# 5️⃣ Join All Streams on tower_id + event_time
# =========================================
joined_df = system_df.alias("sys") \
    .join(radio_df.alias("rad"), ["tower_id", "event_time"], "inner") \
    .join(env_df.alias("env"), ["tower_id", "event_time"], "inner") \
    .join(network_df.alias("net"), ["tower_id", "event_time"], "inner")

# =========================================
# 6️⃣ Select Final Columns for Unified Table
# =========================================
final_df = joined_df.select(
    "event_time",
    "tower_id",
    "region",
    "cpu_pct",
    "memory_pct",
    "power_kw",
    "battery_level_pct",
    "signal_dbm",
    "cell_load_pct",
    "handover_rate",
    "drop_call_rate",
    "temperature_c",
    "humidity_pct",
    "wind_speed_kmh",
    "latency_ms",
    "throughput_mbps",
    "packet_loss",
    "active_users"
)

# =========================================
# 7️⃣ Output Unified Table to Console
# =========================================
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()