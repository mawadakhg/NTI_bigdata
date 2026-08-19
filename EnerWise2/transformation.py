import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType


# Configuration

os.environ["HADOOP_USER_NAME"] = "root"

HDFS_BRONZE = "hdfs://hadoop-namenode:9000/user/root/datalake/bronzeLayer/power_consumption/"
HDFS_SILVER = "hdfs://hadoop-namenode:9000/user/root/datalake/silverLayer/power_consumption/"
GOLD_BASE   = "hdfs://hadoop-namenode:9000/user/root/datalake/goldLayer/"


# spark session creation

spark = SparkSession.builder \
    .appName("PowerETL_Transformation") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .getOrCreate()

print("Spark connected successfully")


# function to write dim tables
# incremental load
def write_dim_incremental(df, path, key_col, name):

    print(f"\n Writing {name}...")

    try:
        df_clean = df.dropDuplicates([key_col])
        # remove duplicates inside current batch

        existing = spark.read.parquet(path).select(key_col).distinct()

        new_only = df_clean.join(
            existing,
            on=key_col,
            how="left_anti"
        )

        count_new = new_only.count()

        if count_new > 0:
            print(f" Adding {count_new} new records to {name}")
            new_only.write.mode("append").parquet(path)
        else:
            print(f" No new records for {name}")
            # only append new records, no overwrite/update on existing keys

    except Exception:
        print(f" First run -> writing full {name}")
        # first batch: dims don't exist yet
        df.dropDuplicates([key_col]).write.mode("overwrite").parquet(path)


# dim_date
def generate_static_date_dim(spark, start_date="2019-01-01", end_date="2030-12-31"):
    df = spark.sql(f"SELECT CAST('{start_date}' AS DATE) as start, CAST('{end_date}' AS DATE) as end")
    df = df.select(
        F.explode(
            F.sequence(F.to_date("start"), F.to_date("end"), F.expr("interval 1 day"))
        ).alias("date")
    )

    dim_date_df = df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").alias("day_of_week"),
        F.weekofyear("date").alias("week_of_year"),
        F.quarter("date").alias("quarter"),
        F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend"))

    return dim_date_df


try:

    # READ BRONZE layer

    print("reading from bronze layer...")
    df = spark.read.parquet(HDFS_BRONZE)
    print(f"Records loaded: {df.count():,}")

    # Dataset has no natural date column — the simulator stamped each row
    # with its own simulated ReadingDate, so derive date_key from that
    # (per-record, not per-batch — gives a real date distribution).
    print("Deriving date_key from ReadingDate...")
    df = df.withColumn("ReadingDate", F.to_date("ReadingDate", "yyyy-MM-dd"))
    df = df.withColumn("date_key", F.date_format("ReadingDate", "yyyyMMdd").cast("int"))

    # drop unimportant columns
    print("Dropping unimportant columns...")
    cols_to_drop = ["source", "batch_id", "ingested_at"]
    df = df.drop(*cols_to_drop)
    print(f"Dropped: {cols_to_drop}")
    print(f"Remaining cols: {df.columns}")

    # removeDuplicatesRow
    print("\n removing duplicates...")
    before = df.count()
    df = df.dropDuplicates(["RecordID"])
    after = df.count()
    print(f"Removed {before - after:,} duplicate rows ({before:,} -> {after:,})")

    # handle null values
    print("\n handling null values...")
    df = df.fillna({
        "RoomArea":            0,
        "NumberofAppliances":  0,
        "OutsideTemperature":  0.0,
        "InsulationThickness": 0.0,
        "AverageTemperature":  0.0,
        "EnergyConsumption":   0.0,
    })
    df = df.fillna({
        "BuildingType": "Unknown",
        "HVACSystem":   "Unknown",
    })
    print("null values handled (numeric -> 0, categorical -> 'Unknown')")

    # dataTypeCasting
    print("\n fixing data types...")
    df = df \
        .withColumn("RoomArea", df["RoomArea"].cast(IntegerType())) \
        .withColumn("NumberofAppliances", df["NumberofAppliances"].cast(IntegerType())) \
        .withColumn("OutsideTemperature", df["OutsideTemperature"].cast(DoubleType())) \
        .withColumn("InsulationThickness", df["InsulationThickness"].cast(DoubleType())) \
        .withColumn("AverageTemperature", df["AverageTemperature"].cast(DoubleType())) \
        .withColumn("EnergyConsumption", df["EnergyConsumption"].cast(DoubleType()))
    print("types fixed")

    # Save Silver layer
    df.write.mode("append").format("parquet").save(HDFS_SILVER)
    print(f"Silver layer written to: {HDFS_SILVER}")

    # star schema -> gold layer
    # Surrogate Key --> using md5

    print(" building Star Schema ...")

    dim_date = generate_static_date_dim(spark)

    # dim_building_type
    dim_building_type = df.select("BuildingType").distinct() \
        .withColumn("building_type_key", F.md5(F.col("BuildingType"))) \
        .select("building_type_key", "BuildingType")

    # dim_hvac_system
    dim_hvac_system = df.select("HVACSystem").distinct() \
        .withColumn("hvac_key", F.md5(F.col("HVACSystem"))) \
        .select("hvac_key", "HVACSystem")

    # fact_energy_consumption => join to reuse dim keys (single source of truth)
    fact_energy_consumption = df \
        .join(dim_building_type, on="BuildingType", how="left") \
        .join(dim_hvac_system,   on="HVACSystem",   how="left") \
        .select(
            "RecordID",
            "building_type_key",
            "hvac_key",
            "date_key",
            "RoomArea",
            "NumberofAppliances",
            "OutsideTemperature",
            "InsulationThickness",
            "AverageTemperature",
            "EnergyConsumption",
        )

    # Write Dims

    print("Writing dim_date...")

    dim_date_path = f"{GOLD_BASE}dim_date"

    try:
        existing_df = spark.read.parquet(dim_date_path)

        if existing_df.count() > 0:
            print("dim_date already populated — skipping")
        else:
            print("dim_date exists but EMPTY — regenerating...")
            dim_date.write.mode("overwrite").parquet(dim_date_path)

    except Exception:
        print("dim_date not found — generating...")
        dim_date.write.mode("overwrite").parquet(dim_date_path)

    print("Writing dim_building_type...")
    write_dim_incremental(
        dim_building_type,
        f"{GOLD_BASE}dim_building_type",
        "building_type_key",
        "dim_building_type")

    print("Writing dim_hvac_system...")
    write_dim_incremental(
        dim_hvac_system,
        f"{GOLD_BASE}dim_hvac_system",
        "hvac_key",
        "dim_hvac_system")

    print("Writing fact_energy_consumption...")
    fact_energy_consumption = fact_energy_consumption.dropDuplicates(["RecordID"])
    fact_energy_consumption.write.mode("overwrite").parquet(f"{GOLD_BASE}fact_energy_consumption")
    print("  fact_energy_consumption ")
    # overwritten every run — this is the *current batch's* fact staging
    # area; loading.py MERGEs it into the permanent Snowflake fact table.

    print("\n Gold Star Schema written successfully.")

except Exception as e:
    print(f"\n  Transformation failed: {e}")
    raise

finally:
    spark.stop()
    print(" Spark session stopped.")

# dim_date          --> Static Dimension (write once, then skip)
# dim_building_type --> SCD Type 0
# dim_hvac_system   --> SCD Type 0