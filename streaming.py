import os
import pickle
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)
from pyspark.sql.functions import (
    from_json, col, to_timestamp, expr,
    when, lit, struct, current_timestamp,
)
from pyspark.sql.functions import pandas_udf

#-----------------------------------------------------------------------------
# AWS_ACCESS_KEY &  AWS_SECRET_KEY 

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

# -----------------------------------------------------------------------------
# 1. Spark Session
spark = (
    SparkSession.builder
    .appName("TelecomTowerPipeline")
    .master("local[2]")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3",
            "org.apache.hadoop:hadoop-aws:3.4.2",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
            "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
        ]),
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # ── Glue Catalog ──────────────────────────────────────────
    .config("spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.defaultCatalog", "glue_catalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse",
            "s3a://telecom-tower-lakehouse/")
    .config("spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.client.region", "us-east-1")
    .config("spark.sql.catalog.glue_catalog.glue.skip-archive", "true")
    # ── S3A ───────────────────────────────────────────────────
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.access.key",  AWS_ACCESS_KEY)   
    .config("spark.hadoop.fs.s3a.secret.key",  AWS_SECRET_KEY)   
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(" Spark Session Created")

#-----------------------------------------------------------------------
# 2 Schemas
system_schema = StructType([
    StructField("tower_id",          StringType(),  True),
    StructField("region",            StringType(),  True),
    StructField("event_time",        StringType(),  True),
    StructField("cpu_pct",           IntegerType(), True),
    StructField("memory_pct",        IntegerType(), True),
    StructField("power_kw",          DoubleType(),  True),
    StructField("battery_level_pct", DoubleType(),  True),
    StructField("tower_status",      StringType(),  True),
])

radio_schema = StructType([
    StructField("tower_id",      StringType(),  True),
    StructField("region",        StringType(),  True),
    StructField("event_time",    StringType(),  True),
    StructField("signal_dbm",    IntegerType(), True),
    StructField("cell_load_pct", IntegerType(), True),
    StructField("handover_rate", DoubleType(),  True),
    StructField("drop_call_rate",DoubleType(),  True),
    StructField("tower_status",  StringType(),  True),
])

environment_schema = StructType([
    StructField("tower_id",      StringType(),  True),
    StructField("region",        StringType(),  True),
    StructField("event_time",    StringType(),  True),
    StructField("temperature_c", DoubleType(),  True),
    StructField("humidity_pct",  DoubleType(),  True),
    StructField("wind_speed_kmh",DoubleType(),  True),
    StructField("tower_status",  StringType(),  True),
])

network_schema = StructType([
    StructField("tower_id",       StringType(),  True),
    StructField("region",         StringType(),  True),
    StructField("event_time",     StringType(),  True),
    StructField("latency_ms",     IntegerType(), True),
    StructField("throughput_mbps",IntegerType(), True),
    StructField("packet_loss",    DoubleType(),  True),
    StructField("active_users",   IntegerType(), True),
    StructField("tower_status",   StringType(),  True),
])

print(" Schemas Defined")

# -----------------------------------------------
# 3. Kafka → Bronze  (Read + Write)


KAFKA_BOOTSTRAP = "localhost:9092"
S3_BASE         = "s3a://telecom-tower-lakehouse"


def read_kafka_topic(topic: str, schema: StructType):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )


system_df      = read_kafka_topic("telecom.tower.system",      system_schema)
radio_df       = read_kafka_topic("telecom.tower.radio",       radio_schema)
environment_df = read_kafka_topic("telecom.tower.environment", environment_schema)
network_df     = read_kafka_topic("telecom.tower.network",     network_schema)

print(" Kafka Streams Ready")

#-------------------------------------------------
#  create  Bronze Tables 

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.telecom_bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.telecom_bronze.system (
        tower_id STRING, region STRING, event_time STRING,
        cpu_pct INT, memory_pct INT, power_kw DOUBLE,
        battery_level_pct DOUBLE, tower_status STRING
    ) USING iceberg
    LOCATION 's3a://telecom-tower-lakehouse/bronze/system/'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.telecom_bronze.radio (
        tower_id STRING, region STRING, event_time STRING,
        signal_dbm INT, cell_load_pct INT, handover_rate DOUBLE,
        drop_call_rate DOUBLE, tower_status STRING
    ) USING iceberg
    LOCATION 's3a://telecom-tower-lakehouse/bronze/radio/'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.telecom_bronze.environment (
        tower_id STRING, region STRING, event_time STRING,
        temperature_c DOUBLE, humidity_pct DOUBLE,
        wind_speed_kmh DOUBLE, tower_status STRING
    ) USING iceberg
    LOCATION 's3a://telecom-tower-lakehouse/bronze/environment/'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.telecom_bronze.network (
        tower_id STRING, region STRING, event_time STRING,
        latency_ms INT, throughput_mbps INT, packet_loss DOUBLE,
        active_users INT, tower_status STRING
    ) USING iceberg
    LOCATION 's3a://telecom-tower-lakehouse/bronze/network/'
""")

print(" Bronze Tables Created")

#----------------------------------------------------------
# write in bronze 
def write_bronze(df, name: str):
    return (
        df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", f"glue_catalog.telecom_bronze.{name}")
        .option("checkpointLocation",
                f"{S3_BASE}/checkpoints/bronze/{name}/")
        .trigger(processingTime="30 seconds")
        .start()
    )


bronze_system      = write_bronze(system_df,      "system")
bronze_radio       = write_bronze(radio_df,        "radio")
bronze_environment = write_bronze(environment_df,  "environment")
bronze_network     = write_bronze(network_df,      "network")

print(" Bronze Layer Started")





#---------------------------------------------------------------------
# 4. Silver Layer  (Bronze → Join → Clean → Silver)
# -----------------------------------------------------------------------------

#   read from  Bronze  
system_s      = spark.readStream.table("glue_catalog.telecom_bronze.system")
radio_s       = spark.readStream.table("glue_catalog.telecom_bronze.radio")
environment_s = spark.readStream.table("glue_catalog.telecom_bronze.environment")
network_s     = spark.readStream.table("glue_catalog.telecom_bronze.network")

print(" Silver reading from Bronze...")


def parse_event_time(df):
    return df.withColumn("event_time", to_timestamp(col("event_time")))


system_ts      = parse_event_time(system_s)
radio_ts       = parse_event_time(radio_s)
environment_ts = parse_event_time(environment_s)
network_ts     = parse_event_time(network_s)


#-------------------------------------------------------------------
#  Watermark 
system_w      = system_ts.withWatermark("event_time", "2 minutes")
radio_w       = radio_ts.withWatermark("event_time",  "2 minutes")
environment_w = environment_ts.withWatermark("event_time", "2 minutes")
network_w     = network_ts.withWatermark("event_time", "2 minutes")


#------------------------------------------------------------------
#  Rename columns قبل الـ Join 
system_w = system_w.select(
    "tower_id",
    col("event_time").alias("event_time_sys"),
    "region",
    "cpu_pct", "memory_pct", "power_kw", "battery_level_pct",
    col("tower_status").alias("status_system"),
)

radio_w = radio_w.select(
    col("tower_id").alias("tower_id_r"),
    col("event_time").alias("event_time_rad"),
    "signal_dbm", "cell_load_pct", "handover_rate", "drop_call_rate",
    col("tower_status").alias("status_radio"),
)

environment_w = environment_w.select(
    col("tower_id").alias("tower_id_e"),
    col("event_time").alias("event_time_env"),
    "temperature_c", "humidity_pct", "wind_speed_kmh",
    col("tower_status").alias("status_env"),
)

network_w = network_w.select(
    col("tower_id").alias("tower_id_n"),
    col("event_time").alias("event_time_net"),
    "latency_ms", "throughput_mbps", "packet_loss", "active_users",
    col("tower_status").alias("status_net"),
)


#----------------------------------------------------------------------------
# Join 
joined_df = (
    system_w
    .join(radio_w,
          (col("tower_id") == col("tower_id_r")) &
          col("event_time_sys").between(
              col("event_time_rad") - expr("INTERVAL 5 MINUTES"),
              col("event_time_rad") + expr("INTERVAL 5 MINUTES"),
          ),
          "left")
    .join(environment_w,
          (col("tower_id") == col("tower_id_e")) &
          col("event_time_sys").between(
              col("event_time_env") - expr("INTERVAL 5 MINUTES"),
              col("event_time_env") + expr("INTERVAL 5 MINUTES"),
          ),
          "left")
    .join(network_w,
          (col("tower_id") == col("tower_id_n")) &
          col("event_time_sys").between(
              col("event_time_net") - expr("INTERVAL 5 MINUTES"),
              col("event_time_net") + expr("INTERVAL 5 MINUTES"),
          ),
          "left")
)

# ── Silver DataFrame ──────────────────────────────────────────
silver_df = joined_df.select(
    col("tower_id"),
    col("region"),
    col("event_time_sys").alias("event_time"),
    "cpu_pct", "memory_pct", "power_kw", "battery_level_pct",
    "signal_dbm", "cell_load_pct", "handover_rate", "drop_call_rate",
    "temperature_c", "humidity_pct", "wind_speed_kmh",
    "latency_ms", "throughput_mbps", "packet_loss", "active_users",
    when(
        (col("status_system") == "critical") |
        (col("status_radio")  == "critical") |
        (col("status_env")    == "critical") |
        (col("status_net")    == "critical"),
        lit("critical"),
    ).when(
        (col("status_system") == "degraded") |
        (col("status_radio")  == "degraded") |
        (col("status_env")    == "degraded") |
        (col("status_net")    == "degraded"),
        lit("degraded"),
    ).otherwise(lit("online")).alias("tower_status"),
)

print(" Silver DataFrame Ready")


#-----------------------------------------------------------
# create  Silver Table 
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.telecom_silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.telecom_silver.silver (
        tower_id          STRING,
        region            STRING,
        event_time        TIMESTAMP,
        cpu_pct           INT,
        memory_pct        INT,
        power_kw          DOUBLE,
        battery_level_pct DOUBLE,
        signal_dbm        INT,
        cell_load_pct     INT,
        handover_rate     DOUBLE,
        drop_call_rate    DOUBLE,
        temperature_c     DOUBLE,
        humidity_pct      DOUBLE,
        wind_speed_kmh    DOUBLE,
        latency_ms        INT,
        throughput_mbps   INT,
        packet_loss       DOUBLE,
        active_users      INT,
        tower_status      STRING
    ) USING iceberg
    PARTITIONED BY (region)
    LOCATION 's3a://telecom-tower-lakehouse/silver/'
""")

print(" Silver Table Created!")

# -------------------------------------------------------------
#  Silver Write in s3 

silver_query = (
    silver_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("path", "glue_catalog.telecom_silver.silver")
    .option("checkpointLocation", f"{S3_BASE}/checkpoints/silver/")
    .trigger(processingTime="30 seconds")
    .start()
)

print(" Silver Layer Started")






# ---------------------------------------------------------------------------
# 5. Gold Layer  (Silver → ML Model → Gold)
# ---------------------------------------------------------------------------

#  تحميل الـ Model 
MODEL_PATH = "/home/ahmed-refat/Desktop/Telecom_Streem/model.pkl"
with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

bc_model = spark.sparkContext.broadcast(model)
print(" Model Loaded & Broadcast")



FEATURES = [
    "cpu_pct", "memory_pct", "power_kw", "battery_level_pct",
    "signal_dbm", "cell_load_pct", "handover_rate", "drop_call_rate",
    "temperature_c", "humidity_pct", "wind_speed_kmh",
    "latency_ms", "throughput_mbps", "packet_loss", "active_users",
]

#-------------------------------------------------------------------------
# Pandas UDFs 


@pandas_udf(DoubleType())
def get_anomaly_score(*cols: pd.Series) -> pd.Series:
    m  = bc_model.value
    df = pd.concat(cols, axis=1)
    df.columns = FEATURES
    return pd.Series(m.decision_function(df))


@pandas_udf(IntegerType())
def get_is_anomaly(*cols: pd.Series) -> pd.Series:
    m    = bc_model.value
    df   = pd.concat(cols, axis=1)
    df.columns = FEATURES
    preds = m.predict(df)
    return pd.Series([0 if p == 1 else 1 for p in preds])


print("UDFs Ready")


#------------------------------------------------------------
#  read from silver layer 
silver_stream = spark.readStream.table("glue_catalog.telecom_silver.silver")

#  Apply Model 
feature_cols = [col(f) for f in FEATURES]

gold_df = (
    silver_stream
    .withColumn("anomaly_score", get_anomaly_score(*feature_cols))
    .withColumn("is_anomaly",    get_is_anomaly(*feature_cols))
)

print(" Gold DataFrame Ready")


#-----------------------------------------
#  create Gold Table 
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.telecom_gold")

spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.telecom_gold.tower_metrics (
        tower_id          STRING,
        region            STRING,
        event_time        TIMESTAMP,
        cpu_pct           INT,
        memory_pct        INT,
        power_kw          DOUBLE,
        battery_level_pct DOUBLE,
        signal_dbm        INT,
        cell_load_pct     INT,
        handover_rate     DOUBLE,
        drop_call_rate    DOUBLE,
        temperature_c     DOUBLE,
        humidity_pct      DOUBLE,
        wind_speed_kmh    DOUBLE,
        latency_ms        INT,
        throughput_mbps   INT,
        packet_loss       DOUBLE,
        active_users      INT,
        tower_status      STRING,
        anomaly_score     DOUBLE,
        is_anomaly        INT
    ) USING iceberg
    PARTITIONED BY (region)
    LOCATION 's3a://telecom-tower-lakehouse/gold/'
""")

print(" Gold Table Created")


#----------------------------------------------------------------------
#   Write in gold layer 
gold_query = (
    gold_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("path", "glue_catalog.telecom_gold.tower_metrics")
    .option("checkpointLocation", f"{S3_BASE}/checkpoints/gold/")
    .trigger(processingTime="30 seconds")
    .start()
)

print(" Gold Layer Started!")

# --------------------------------------------------------
# 6. Keep All Streams Running

try:
    for q in spark.streams.active:
        q.awaitTermination()
except KeyboardInterrupt:
    print(" Stopping all streams...")
    for q in spark.streams.active:
        q.stop()
    spark.stop()
