from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType
)



spark = SparkSession.builder \
    .appName("EnerWise-ETL-Processing") \
    .getOrCreate()

print("====================================")
print("EnerWise Spark ETL Started")
print("====================================")

input_path = "hdfs://namenode:9000/enerwise/raw/energy"

cleaned_output = "hdfs://namenode:9000/enerwise/processed/energy"

analytics_base = "hdfs://namenode:9000/enerwise/analytics"

ml_output = analytics_base + "/ml_features"


print("\n====================================")
print("Reading Raw Data")
print("====================================")


df = spark.read.json(input_path)

print("\nOriginal Schema:")
df.printSchema()

raw_count = df.count()

print("\nOriginal Record Count:", raw_count)

print("\nSample Raw Data:")
df.show(10, truncate=False)



# Normalize Column Names


df = df \
    .withColumnRenamed("Outside Temperature", "OutsideTemperature")



# Convert Numeric Columns Safely


print("Cleaning Numeric Columns")


numeric_columns = [
    "RoomArea",
    "NumberofAppliances",
    "OutsideTemperature",
    "InsulationThickness",
    "AverageTemperature",
    "EnergyConsumption"
]


for c in numeric_columns:

    # Convert NaN and Infinity to NULL first
    df = df.withColumn(
        c,
        F.when(
            F.isnan(F.col(c)),
            None
        ).otherwise(F.col(c))
    )

    df = df.withColumn(
        c,
        F.when(
            F.col(c).isin(float("inf"), float("-inf")),
            None
        ).otherwise(F.col(c))
    )


# Explicit Numeric Casting


for c in numeric_columns:

    df = df.withColumn(
        c,
        F.col(c).cast(DoubleType())
    )


#  Clean NumberofAppliances


df = df.withColumn(
    "NumberofAppliances",
    F.when(
        F.col("NumberofAppliances").isNull(),
        None
    ).otherwise(
        F.col("NumberofAppliances")
    )
)


# Convert only valid values to integer
df = df.withColumn(
    "NumberofAppliances",
    F.floor(F.col("NumberofAppliances")).cast(IntegerType())
)



#  Clean Categorical Columns


print("\n====================================")
print("Cleaning Categorical Columns")
print("====================================")

df = df \
    .withColumn(
        "BuildingType",
        F.trim(F.col("BuildingType"))
    ) \
    .withColumn(
        "HVACSystem",
        F.trim(F.col("HVACSystem"))
    )


# Convert empty strings to NULL

df = df \
    .withColumn(
        "BuildingType",
        F.when(
            F.col("BuildingType") == "",
            None
        ).otherwise(F.col("BuildingType"))
    ) \
    .withColumn(
        "HVACSystem",
        F.when(
            F.col("HVACSystem") == "",
            None
        ).otherwise(F.col("HVACSystem"))
    )


# Standardize BuildingType

df = df.withColumn(
    "BuildingType",
    F.initcap(F.col("BuildingType"))
)


# Standardize HVAC

df = df.withColumn(
    "HVACSystem",
    F.initcap(F.col("HVACSystem"))
)


# Fix HVAC acronyms

hvac_fix_map = {
    "Central Ac": "Central AC",
    "Split Ac": "Split AC",
    "No Hvac": "No HVAC",
    "Vrf": "VRF"
}


for wrong, right in hvac_fix_map.items():

    df = df.withColumn(
        "HVACSystem",
        F.when(
            F.col("HVACSystem") == wrong,
            right
        ).otherwise(
            F.col("HVACSystem")
        )
    )


#  Check Missing Values BEFORE Filtering


print("\n====================================")
print("Missing Values Before Cleaning")
print("====================================")

for c in numeric_columns + ["BuildingType", "HVACSystem"]:

    missing = df.filter(
        F.col(c).isNull()
    ).count()

    print(
        f"{c}: {missing} missing values"
    )



#  Remove Duplicates


before_dedup = df.count()

df = df.dropDuplicates()

after_dedup = df.count()

print("\n====================================")
print("Duplicate Removal")
print("====================================")

print(
    "Duplicates Removed:",
    before_dedup - after_dedup
)


#  Remove Missing Required Values

required_columns = [
    "RoomArea",
    "NumberofAppliances",
    "OutsideTemperature",
    "InsulationThickness",
    "BuildingType",
    "HVACSystem",
    "AverageTemperature",
    "EnergyConsumption"
]


before_dropna = df.count()

df = df.dropna(
    subset=required_columns
)

after_dropna = df.count()

print("\n====================================")
print("Missing Values Removal")
print("====================================")

print(
    "Records Removed:",
    before_dropna - after_dropna
)

print(
    "Records Remaining:",
    after_dropna
)


#  Remove Invalid Numeric Values


before_invalid = df.count()


# RoomArea must be > 0
df = df.filter(
    F.col("RoomArea") > 0
)


# EnergyConsumption must be > 0
df = df.filter(
    F.col("EnergyConsumption") > 0
)


# NumberofAppliances >= 0
df = df.filter(
    F.col("NumberofAppliances") >= 0
)


# InsulationThickness >= 0
df = df.filter(
    F.col("InsulationThickness") >= 0
)


after_invalid = df.count()

print("\n====================================")
print("Invalid Numeric Values")
print("====================================")

print(
    "Records Removed:",
    before_invalid - after_invalid
)

print(
    "Records Remaining:",
    after_invalid
)


#  Temperature Validation


before_temperature = df.count()


df = df.filter(
    F.col("OutsideTemperature").between(-50, 60)
)

df = df.filter(
    F.col("AverageTemperature").between(-30, 50)
)


after_temperature = df.count()

print("\n====================================")
print("Temperature Validation")
print("====================================")

print(
    "Records Removed:",
    before_temperature - after_temperature
)

print(
    "Records Remaining:",
    after_temperature
)


# Statistical Outlier Detection


print("\n====================================")
print("IQR Outlier Detection")
print("====================================")


outlier_columns = [
    "RoomArea",
    "NumberofAppliances",
    "EnergyConsumption"
]


outlier_condition = None


for c in outlier_columns:

    quantiles = df.approxQuantile(
        c,
        [0.25, 0.75],
        0.01
    )

    if len(quantiles) < 2:

        print(
            f"WARNING: Cannot calculate IQR for {c}"
        )

        continue


    q1 = quantiles[0]
    q3 = quantiles[1]

    iqr = q3 - q1


    lower_bound = q1 - 3 * iqr
    upper_bound = q3 + 3 * iqr


    print(
        f"{c}: "
        f"Q1={q1:.2f}, "
        f"Q3={q3:.2f}, "
        f"Lower={lower_bound:.2f}, "
        f"Upper={upper_bound:.2f}"
    )


    condition = (
        F.col(c) >= lower_bound
    ) & (
        F.col(c) <= upper_bound
    )


    if outlier_condition is None:

        outlier_condition = condition

    else:

        outlier_condition = (
            outlier_condition & condition
        )


before_outliers = df.count()


if outlier_condition is not None:

    df = df.filter(
        outlier_condition
    )


after_outliers = df.count()


print(
    "Records Removed:",
    before_outliers - after_outliers
)

print(
    "Records Remaining:",
    after_outliers
)


# Final Cleaned DataFrame


df_clean = df


print("\n====================================")
print("FINAL CLEANED DATA")
print("====================================")

clean_count = df_clean.count()

print(
    "Raw Records:",
    raw_count
)

print(
    "Clean Records:",
    clean_count
)


if raw_count > 0:

    print(
        "Retention Rate:",
        f"{clean_count / raw_count:.2%}"
    )


print("\nCleaned Schema:")

df_clean.printSchema()


print("\nCleaned Data:")

df_clean.show(
    10,
    truncate=False
)


#  SAFETY CHECK


if clean_count == 0:

    print("\n")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("WARNING: CLEANED DATAFRAME IS EMPTY")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("\n")

    print(
        "Check the raw data and cleaning conditions."
    )

    spark.stop()

    raise Exception(
        "ETL stopped because no records remained after cleaning."
    )


#  Feature Engineering


print("\n====================================")
print("Feature Engineering")
print("====================================")


df_features = df_clean \
    .withColumn(
        "EnergyPerArea",
        F.round(
            F.col("EnergyConsumption") /
            F.col("RoomArea"),
            4
        )
    ) \
    .withColumn(
        "TemperatureDifference",
        F.round(
            F.col("AverageTemperature") -
            F.col("OutsideTemperature"),
            4
        )
    )


df_features.cache()


print("\nFeature Dataset:")

df_features.show(
    10,
    truncate=False
)


#  Energy Statistics


print("\n====================================")
print("Energy Statistics")
print("====================================")


energy_statistics = df_features.select(

    F.round(
        F.avg("EnergyConsumption"),
        2
    ).alias("AverageEnergy"),

    F.round(
        F.min("EnergyConsumption"),
        2
    ).alias("MinimumEnergy"),

    F.round(
        F.max("EnergyConsumption"),
        2
    ).alias("MaximumEnergy"),

    F.round(
        F.sum("EnergyConsumption"),
        2
    ).alias("TotalEnergy"),

    F.count("*").alias(
        "TotalRecords"
    )
)


energy_statistics.show(
    truncate=False
)


# Energy By Building Type


print("\n====================================")
print("Energy By Building Type")
print("====================================")


energy_by_building = df_features.groupBy(
    "BuildingType"
).agg(

    F.count("*").alias(
        "RecordCount"
    ),

    F.round(
        F.avg("EnergyConsumption"),
        2
    ).alias(
        "AverageEnergy"
    ),

    F.round(
        F.sum("EnergyConsumption"),
        2
    ).alias(
        "TotalEnergy"
    ),

    F.round(
        F.avg("EnergyPerArea"),
        4
    ).alias(
        "AverageEnergyPerArea"
    )
)


energy_by_building.show(
    truncate=False
)


#  Energy By HVAC


print("\n====================================")
print("Energy By HVAC System")
print("====================================")


energy_by_hvac = df_features.groupBy(
    "HVACSystem"
).agg(

    F.count("*").alias(
        "RecordCount"
    ),

    F.round(
        F.avg("EnergyConsumption"),
        2
    ).alias(
        "AverageEnergy"
    ),

    F.round(
        F.sum("EnergyConsumption"),
        2
    ).alias(
        "TotalEnergy"
    ),

    F.round(
        F.avg("EnergyPerArea"),
        4
    ).alias(
        "AverageEnergyPerArea"
    )
)


energy_by_hvac.show(
    truncate=False
)


#  Energy Efficiency


print("\n====================================")
print("Energy Efficiency")
print("====================================")


efficiency = df_features.select(

    "BuildingType",

    "HVACSystem",

    "RoomArea",

    "EnergyConsumption",

    "EnergyPerArea"

)


efficiency.orderBy(
    F.col("EnergyPerArea").desc()
).show(
    10,
    truncate=False
)


#  Save Cleaned Data


print("\n====================================")
print("Saving Cleaned Data")
print("====================================")


df_features.write \
    .mode("overwrite") \
    .json(cleaned_output)


print(
    "Cleaned data saved to:"
)

print(
    cleaned_output
)


# Save Analytics


print("\n====================================")
print("Saving Analytics")
print("====================================")


energy_statistics.write \
    .mode("overwrite") \
    .json(
        analytics_base +
        "/energy_statistics"
    )


energy_by_building.write \
    .mode("overwrite") \
    .json(
        analytics_base +
        "/energy_by_building"
    )


energy_by_hvac.write \
    .mode("overwrite") \
    .json(
        analytics_base +
        "/energy_by_hvac"
    )


print(
    "Analytics saved successfully."
)


#  Prepare ML Features


print("\n====================================")
print("Preparing ML Features")
print("====================================")


from pyspark.ml.feature import (
    StringIndexer,
    VectorAssembler
)


building_indexer = StringIndexer(
    inputCol="BuildingType",
    outputCol="BuildingType_index",
    handleInvalid="keep"
)


hvac_indexer = StringIndexer(
    inputCol="HVACSystem",
    outputCol="HVACSystem_index",
    handleInvalid="keep"
)


ml_df = building_indexer.fit(
    df_features
).transform(
    df_features
)


ml_df = hvac_indexer.fit(
    ml_df
).transform(
    ml_df
)


assembler = VectorAssembler(

    inputCols=[

        "AverageTemperature",

        "OutsideTemperature",

        "InsulationThickness",

        "NumberofAppliances",

        "RoomArea",

        "BuildingType_index",

        "HVACSystem_index"

    ],

    outputCol="features",

    handleInvalid="skip"
)


ml_features = assembler.transform(
    ml_df
)


ml_features = ml_features.select(

    "features",

    "EnergyConsumption"

)


print("\nML Dataset:")

ml_features.show(
    10,
    truncate=False
)


ml_count = ml_features.count()


print(
    "\nML Record Count:",
    ml_count
)


#  Save ML Dataset


print("\n====================================")
print("Saving ML Features")
print("====================================")


ml_features.write \
    .mode("overwrite") \
    .parquet(
        ml_output
    )


print(
    "ML Features saved to:"
)

print(
    ml_output
)


#  Final Summary


print("\n====================================")
print("EnerWise ETL SUMMARY")
print("====================================")

print(
    "Raw Records:",
    raw_count
)

print(
    "Clean Records:",
    clean_count
)

print(
    "ML Records:",
    ml_count
)

print(
    "Cleaned Data:",
    cleaned_output
)

print(
    "Analytics:",
    analytics_base
)

print(
    "ML Features:",
    ml_output
)




df_features.unpersist()

spark.stop()


print("\n====================================")
print("EnerWise Spark ETL Completed")
print("====================================")
