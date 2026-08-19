import os
from pyspark.sql import SparkSession


# configuration

os.environ["HADOOP_USER_NAME"] = "root"

GOLD_BASE   = "hdfs://hadoop-namenode:9000/user/root/datalake/goldLayer/"

# snowflake connection
# ⚠️ Replace these with your own account details. Do NOT commit real
# credentials into source control — use Airflow Connections/Variables
# or environment variables instead, e.g. os.environ["SF_PASSWORD"].
sf_options = {
    "sfURL": "bh47951.af-south-1.aws.snowflakecomputing.com",
    "sfUser":"roaa1",
    "sfPassword":  "roroQWE1234567@",
    "sfDatabase":  "POWER_DW",
    "sfSchema":    "GOLD_LAYER",
    "sfWarehouse": "POWER_WH",
}

# spark session
spark = SparkSession.builder \
    .appName("PowerETL_Loading") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host", "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.15.0") \
    .getOrCreate()  # Snowflake connector loaded by default

print("Spark connected successfully")


def load_dim_once_to_snowflake(table_name):

    df = spark.read.parquet(f"{GOLD_BASE}{table_name}")

    try:
        count_query = f"SELECT COUNT(*) AS cnt FROM POWER_DW.GOLD_LAYER.{table_name.upper()}"

        existing_df = spark.read \
            .format("snowflake") \
            .options(**sf_options) \
            .option("query", count_query) \
            .load()

        cnt = existing_df.collect()[0]["CNT"]

        if cnt > 0:
            print(f"{table_name.upper()} already populated — skipping load")
            return

        print(f"{table_name.upper()} exists but EMPTY — loading data...")

    except Exception:
        print(f"{table_name.upper()} not found — creating and loading...")

    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", f"POWER_DW.GOLD_LAYER.{table_name.upper()}") \
        .mode("overwrite") \
        .save()

    print(f"{table_name.upper()} loaded successfully")
    # for dims like dim_date that never change once populated


# def load_to_snowflake(table_name):
#     # atomic swap load for append-only lookup dims
#     # (dim_building_type, dim_hvac_system — accumulated in HDFS gold via
#     # write_dim_incremental, so the full parquet already has everything)

#     temp_table  = f"POWER_DW.GOLD_LAYER.{table_name.upper()}_TEMP"
#     final_table = f"POWER_DW.GOLD_LAYER.{table_name.upper()}"

#     print(f"\n  Starting Atomic Load for {final_table}...")

#     print(f"  Reading {table_name} from HDFS...")
#     df = spark.read.parquet(f"{GOLD_BASE}{table_name}")
#     print(f"  Records: {df.count():,}")

#     print(f"  Writing to temp table: {temp_table}...")
#     df.write \
#         .format("net.snowflake.spark.snowflake") \
#         .options(**sf_options) \
#         .option("dbtable", temp_table) \
#         .mode("overwrite") \
#         .save()
#     try:
#         snowflake_utils = spark._jvm.net.snowflake.spark.snowflake.Utils

#         swap_query = f"ALTER TABLE IF EXISTS {final_table} SWAP WITH {temp_table}"
#         snowflake_utils.runQuery(sf_options, swap_query)
#         print(f"Swapped {temp_table} -> {final_table}")

#         drop_query = f"DROP TABLE IF EXISTS {temp_table}"
#         snowflake_utils.runQuery(sf_options, drop_query)
#         print(f"Temp table dropped")

#         print(f"SUCCESS: {final_table} loaded atomically.")

#     except Exception as e:
#         print(f"Atomic Swap failed for {final_table}: {e}")
#         raise e
# atomic swap
# FINAL = old data
# TEMP  = new data (old + appended data, already merged in HDFS gold)
# after swap:
# FINAL <- TEMP (new data)
# TEMP  <- OLD FINAL

def load_dimension_to_snowflake(table_name):
    print(f"\n  Loading dimension: {table_name}")

    # Read dimension from HDFS Gold
    df = spark.read.parquet(f"{GOLD_BASE}{table_name}")

    count = df.count()
    print(f"  Records in HDFS Gold: {count:,}")

    if count == 0:
        print(f"  No data found for {table_name}")
        return

    target_table = f"POWER_DW.GOLD_LAYER.{table_name.upper()}"

    print(f"  Writing to Snowflake: {target_table}")

    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", target_table) \
        .mode("overwrite") \
        .save()

    print(f"  {table_name.upper()} loaded successfully")


def load_fact_to_snowflake(table_name):

    target_table = table_name.upper()
    staging_table = f"{target_table}_STG"

    print(f"\n Starting MERGE Load for {target_table}...")

    df = spark.read.parquet(f"{GOLD_BASE}{table_name}")

    # write staging
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", staging_table) \
        .mode("overwrite") \
        .save()

    snowflake_utils = spark._jvm.net.snowflake.spark.snowflake.Utils

    # check if table exists
    check_query = f"""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{target_table}'
    """

    exists = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", check_query) \
        .load() \
        .collect()[0][0] > 0

    try:
        if not exists:
            print(" First run -> creating table with full load")

            df.write \
                .format("net.snowflake.spark.snowflake") \
                .options(**sf_options) \
                .option("dbtable", target_table) \
                .mode("overwrite") \
                .save()

        else:
            print(" Table exists -> running MERGE")

            merge_query = f"""
            MERGE INTO POWER_DW.GOLD_LAYER.{target_table} t
            USING POWER_DW.GOLD_LAYER.{staging_table} s
            ON t.RecordID = s.RecordID

            WHEN MATCHED THEN UPDATE SET
                t.building_type_key = s.building_type_key,
                t.hvac_key = s.hvac_key,
                t.date_key = s.date_key,
                t.RoomArea = s.RoomArea,
                t.NumberofAppliances = s.NumberofAppliances,
                t.OutsideTemperature = s.OutsideTemperature,
                t.InsulationThickness = s.InsulationThickness,
                t.AverageTemperature = s.AverageTemperature,
                t.EnergyConsumption = s.EnergyConsumption

            WHEN NOT MATCHED THEN INSERT
            VALUES (
                s.RecordID,
                s.building_type_key,
                s.hvac_key,
                s.date_key,
                s.RoomArea,
                s.NumberofAppliances,
                s.OutsideTemperature,
                s.InsulationThickness,
                s.AverageTemperature,
                s.EnergyConsumption
            )
            """

            snowflake_utils.runQuery(sf_options, merge_query)

        # cleanup
        snowflake_utils.runQuery(sf_options, f"DROP TABLE IF EXISTS {staging_table}")

        print(" SUCCESS")

    except Exception as e:
        print(f" Failed: {e}")
        raise e

# adds the current batch's fact rows into staging, then MERGEs new/changed
# records into the permanent fact table (delta load):
#   - table doesn't exist yet -> overwrite (first run)
#   - table exists -> MERGE (update matches, insert new)


# run
try:
    print("\n" + "=" * 55)
    print("  SNOWFLAKE LOAD — Gold Layer -> Snowflake")
    print("=" * 55)

    # Dims — Atomic Swap
    load_dim_once_to_snowflake("dim_date")   # static — skip if already populated
    load_dimension_to_snowflake("dim_building_type")
    load_dimension_to_snowflake("dim_hvac_system")

    # fact
    load_fact_to_snowflake("fact_energy_consumption")

    print("\n" + "=" * 55)
    print(" ALL GOLD TABLES LOADED TO SNOWFLAKE")
    print("=" * 55 + "\n")

except Exception as e:
    print(f"\n Loading failed: {e}")
    raise e

finally:
    spark.stop()
    print("Spark session stopped.")