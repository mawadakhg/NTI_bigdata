import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

# configuration
os.environ["HADOOP_USER_NAME"] = "root"
HDFS_RAW_ZONE ="/user/root/datalake/raw1/power_consumption/"

HDFS_BRONZE = "hdfs://hadoop-namenode:9000/user/root/datalake/bronzeLayer/power_consumption/"

# Airflow بيبعت مسار الـ file جوه HDFS raw zone كـ argument
# ['extraction_power.py', '/user/root/datalake/raw1/power_consumption/power_batch_0001.json']
# sys.argv[0] --> name of file
# sys.argv[1] --> hdfs raw path of batch
if len(sys.argv) < 2:
    print(" No batch file provided. Usage: extraction_power.py <hdfs_raw_batch_path>")
    sys.exit(1)

batch_file = sys.argv[1]
batch_name = os.path.basename(batch_file)

print(f"\n  Batch file : {batch_name}")
print(f"  Reading from HDFS raw zone: {batch_file}")

# expected_schema
schema = StructType([
    StructField("RecordID",            StringType(),  True),
    StructField("RoomArea",            IntegerType(), True),
    StructField("NumberofAppliances",  IntegerType(), True),
    StructField("OutsideTemperature",  DoubleType(),  True),
    StructField("InsulationThickness", DoubleType(),  True),
    StructField("BuildingType",        StringType(),  True),
    StructField("HVACSystem",          StringType(),  True),
    StructField("AverageTemperature",  DoubleType(),  True),
    StructField("EnergyConsumption",   DoubleType(),  True),
    StructField("ReadingDate",         StringType(),  True),
    StructField("source",              StringType(),  True),
    StructField("ingested_at",         StringType(),  True),
    StructField("batch_id",            StringType(),  True),
])

# spark session
spark = SparkSession.builder \
    .appName("PowerETL_Extraction") \
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


# read batch from HDFS raw zone, validate schema, write to HDFS bronze layer

try:
    # batch_file is already an HDFS path (e.g. /user/root/datalake/raw1/...)
    # and spark.hadoop.fs.defaultFS is HDFS, so no "file://" or "hdfs://" prefix needed
    raw_df = spark.read \
        .schema(schema) \
        .json(batch_file)

    record_count = raw_df.count()
    print(f"  Records in batch : {record_count}")

    if record_count == 0:
        print(" Batch is empty — skipping write.")
    else:
        print(f"  Writing to HDFS Bronze: {HDFS_BRONZE}")
        raw_df.write \
            .mode("append") \
            .format("parquet") \
            .save(HDFS_BRONZE)
        print(f"batch written to Bronze layer as Parquet.")

except Exception as e:
    print(f"Extraction failed: {e}")
    raise

finally:
    spark.stop()
    print("Spark session stopped.")