from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ----------------------------
# SPARK SESSION
# ----------------------------
spark = SparkSession.builder \
    .appName("RetailStreamingConsumer") \
    .enableHiveSupport() \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# KINESIS CONFIG
# ----------------------------
STREAM_NAME = "pharma_kinesis_kritika"
REGION = "ap-south-1"
ENDPOINT = "https://kinesis.ap-south-1.amazonaws.com"

# ----------------------------
# READ FROM KINESIS
# ----------------------------
raw_df = spark.readStream \
    .format("aws-kinesis") \
    .option("kinesis.streamName", STREAM_NAME) \
    .option("kinesis.region", REGION) \
    .option("kinesis.endpointUrl", ENDPOINT) \
    .option("kinesis.startingPosition", "LATEST") \
    .load()

# Convert binary → string
json_df = raw_df.selectExpr("CAST(data AS STRING) as json_data")

# ----------------------------
# SCHEMA
# ----------------------------
schema = StructType([
    StructField("source_type", StringType()),

    # inventory
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("distributor_id", StringType()),
    StructField("product_sku", StringType()),
    StructField("batch_id", StringType()),
    StructField("change_type", StringType()),
    StructField("quantity_delta", StringType()),
    StructField("current_stock", StringType()),

    # sales
    StructField("transaction_id", StringType()),
    StructField("pharmacy_id", StringType()),
    StructField("quantity_sold", StringType()),
    StructField("unit_price", StringType()),
    StructField("discount_pct", StringType()),
    StructField("revenue", StringType()),
    StructField("fulfillment_status", StringType()),

    # temperature
    StructField("sensor_id", StringType()),
    StructField("temperature", StringType()),
    StructField("unit", StringType()),
    StructField("humidity", StringType())
])

# ----------------------------
# PARSE JSON
# ----------------------------
parsed_df = json_df \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp
parsed_df = parsed_df.withColumn("event_time", to_timestamp("event_time"))

# Partition column
parsed_df = parsed_df.withColumn("partition_date", to_date("event_time"))

# ----------------------------
# SPLIT STREAMS
# ----------------------------
inventory_df = parsed_df.filter(col("source_type") == "inventory")
sales_df = parsed_df.filter(col("source_type") == "sales")
temperature_df = parsed_df.filter(col("source_type") == "temperature")

# ============================================================
# TEMPERATURE CLEANING
# ============================================================
temp_clean = temperature_df \
    .withColumn("temperature", col("temperature").cast("double")) \
    .withColumn("humidity", col("humidity").cast("double")) \
    .withColumn(
        "temperature_c",
        when(col("unit") == "F", (col("temperature") - 32) * 5/9)
        .otherwise(col("temperature"))
    ) \
    .filter((col("temperature_c") > -50) & (col("temperature_c") < 100)) \
    .select("sensor_id", "temperature", "humidity", "temperature_c", "unit", "event_time", "partition_date")

# ============================================================
# SALES CLEANING
# ============================================================
sales_clean = sales_df \
    .withColumn("quantity_sold", col("quantity_sold").cast("int")) \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("discount_pct", col("discount_pct").cast("double")) \
    .withColumn("revenue", col("revenue").cast("double")) \
    .filter((col("quantity_sold") > 0) & (col("unit_price") > 0)) \
    .select("transaction_id", "pharmacy_id", "quantity_sold", "unit_price", "discount_pct", "revenue", "fulfillment_status", "event_time", "partition_date")

# ============================================================
# INVENTORY CLEANING
# ============================================================
inventory_clean = inventory_df \
    .withColumn("quantity_delta", col("quantity_delta").cast("int")) \
    .withColumn("current_stock", col("current_stock").cast("int")) \
    .filter(col("change_type").isin("RECEIPT", "DISPATCH", "RETURN", "ADJUSTMENT")) \
    .select("event_id", "distributor_id", "product_sku", "batch_id", "change_type", "quantity_delta", "current_stock", "event_time", "partition_date")

# ============================================================
# WRITE STREAMS TO S3 (FINAL FIX)
# ============================================================

# Temperature Stream
temp_query = temp_clean.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3://pharma-data-lake-kritika/streaming_kinesis/temperature") \
    .option("checkpointLocation", "s3://pharma-data-lake-kritika/checkpoints/temperature/") \
    .trigger(processingTime="60 seconds") \
    .start()

# Sales Stream
sales_clean = sales_df \
    .withColumn("quantity_sold", col("quantity_sold").cast("int")) \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("discount_pct", col("discount_pct").cast("double")) \
    .withColumn("revenue", col("revenue").cast("double")) \
    .filter((col("quantity_sold") > 0) & (col("unit_price") > 0)) \
    .select(
        "transaction_id",
        "pharmacy_id",
        "product_sku",   # ✅ ADD THIS
        "batch_id",      # ✅ ADD THIS
        "quantity_sold",
        "unit_price",
        "discount_pct",
        "revenue",
        "fulfillment_status",
        "event_time",
        "partition_date"
    )

# Inventory Stream
inventory_query = inventory_clean.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3://pharma-data-lake-kritika/streaming_kinesis/inventory") \
    .option("checkpointLocation", "s3://pharma-data-lake-kritika/checkpoints/inventory/") \
    .trigger(processingTime="60 seconds") \
    .start()

# ----------------------------
# KEEP STREAM RUNNING
# ----------------------------
spark.streams.awaitAnyTermination()
