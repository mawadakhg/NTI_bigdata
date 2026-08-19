from pyspark.sql import SparkSession

# =========================================================
# 1. Spark Session
# =========================================================

spark = SparkSession.builder \
    .appName("EnerWise-New-Snowflake-Project") \
    .getOrCreate()

print("=" * 60)
print("EnerWise - New Project - HDFS to Snowflake")
print("=" * 60)


# =========================================================
# 2. Snowflake Configuration
# =========================================================

sfOptions = {
    "sfURL": "swalujd-WQ14850.snowflakecomputing.com",
    "sfUser": "MAWADAKHG",
    "sfPassword": "MawadaAnas@2911",
    "sfDatabase": "ENERWISE_NEW",
    "sfSchema": "ANALYTICS",
    "sfWarehouse": "ENERWISE_NEW_WH",
    "sfRole": "ACCOUNTADMIN",
    "sfTimezone": "UTC"
}


# =========================================================
# 3. Function
# =========================================================

def load_to_snowflake(hdfs_path, table_name):

    print("\n" + "=" * 60)
    print(f"Loading: {table_name}")
    print("=" * 60)

    # Read from HDFS
    df = spark.read.json(hdfs_path)

    # Check data
    row_count = df.count()

    print(f"Rows: {row_count}")

    df.printSchema()

    df.show(5, truncate=False)

    # Write to Snowflake
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()

    print(f"SUCCESS: {table_name}")


# =========================================================
# 4. Load Processed Energy
# =========================================================

load_to_snowflake(
    "hdfs://namenode:9000/enerwise/processed/energy",
    "PROCESSED_ENERGY"
)


# =========================================================
# 5. Load Analytics
# =========================================================

load_to_snowflake(
    "hdfs://namenode:9000/enerwise/analytics/energy_by_building",
    "ENERGY_BY_BUILDING"
)

load_to_snowflake(
    "hdfs://namenode:9000/enerwise/analytics/energy_by_hvac",
    "ENERGY_BY_HVAC"
)

load_to_snowflake(
    "hdfs://namenode:9000/enerwise/analytics/energy_statistics",
    "ENERGY_STATISTICS"
)

load_to_snowflake(
    "hdfs://namenode:9000/enerwise/analytics/building_health",
    "BUILDING_HEALTH"
)

load_to_snowflake(
    "hdfs://namenode:9000/enerwise/analytics/ml_predictions",
    "ML_PREDICTIONS"
)


print("\n" + "=" * 60)
print("ALL DATASETS LOADED SUCCESSFULLY")
print("=" * 60)

spark.stop()