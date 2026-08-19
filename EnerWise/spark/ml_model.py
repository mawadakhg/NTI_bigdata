from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


# =========================================================
# 1. Create Spark Session
# =========================================================

spark = SparkSession.builder \
    .appName("EnerWise-ML-Model") \
    .getOrCreate()

print("====================================")
print("EnerWise ML Model Started")
print("====================================")


# =========================================================
# 2. Paths
# =========================================================

input_path = "hdfs://namenode:9000/enerwise/processed/energy"

predictions_output = \
    "hdfs://namenode:9000/enerwise/analytics/ml_predictions"

building_health_output = \
    "hdfs://namenode:9000/enerwise/analytics/building_health"

model_output = \
    "hdfs://namenode:9000/enerwise/models/energy_linear_regression"


# =========================================================
# 3. Read Processed Data from HDFS
# =========================================================

print("\n====================================")
print("Reading Processed Data")
print("====================================")

df = spark.read.json(input_path)

print("\nSchema:")
df.printSchema()

record_count = df.count()

print("\nRecord Count:", record_count)


# =========================================================
# 4. Validate Required Columns
# =========================================================

required_columns = [
    "AverageTemperature",
    "OutsideTemperature",
    "InsulationThickness",
    "NumberofAppliances",
    "RoomArea",
    "TemperatureDifference",
    "BuildingType",
    "HVACSystem",
    "EnergyConsumption",
    "EnergyPerArea"
]

missing_columns = [
    c for c in required_columns
    if c not in df.columns
]

if missing_columns:

    print("\n====================================")
    print("ERROR: Missing Columns")
    print("====================================")

    print("Missing:", missing_columns)
    print("Available:", df.columns)

    spark.stop()

    raise Exception(
        f"Required columns are missing: {missing_columns}"
    )


# =========================================================
# 5. Remove Invalid ML Rows
# =========================================================

print("\n====================================")
print("Preparing ML Data")
print("====================================")

numeric_ml_columns = [
    "AverageTemperature",
    "OutsideTemperature",
    "InsulationThickness",
    "NumberofAppliances",
    "RoomArea",
    "TemperatureDifference",
    "EnergyConsumption",
    "EnergyPerArea"
]

# Convert NaN / Infinity to NULL
for c in numeric_ml_columns:

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
            F.col(c).isin(
                float("inf"),
                float("-inf")
            ),
            None
        ).otherwise(F.col(c))
    )


# Remove rows with missing ML values

df = df.dropna(
    subset=required_columns
)


# Remove impossible values

df = df.filter(
    (F.col("RoomArea") > 0) &
    (F.col("EnergyConsumption") > 0) &
    (F.col("NumberofAppliances") >= 0) &
    (F.col("InsulationThickness") >= 0)
)


ml_record_count = df.count()

print(
    "Valid ML Records:",
    ml_record_count
)


if ml_record_count == 0:

    spark.stop()

    raise Exception(
        "No valid records available for ML."
    )


# =========================================================
# 6. Encode Building Type
# =========================================================

print("\n====================================")
print("Encoding Building Type")
print("====================================")

building_indexer = StringIndexer(
    inputCol="BuildingType",
    outputCol="BuildingType_index",
    handleInvalid="keep"
)

building_encoder = OneHotEncoder(
    inputCol="BuildingType_index",
    outputCol="BuildingType_ohe"
)


# =========================================================
# 7. Encode HVAC System
# =========================================================

print("\n====================================")
print("Encoding HVAC System")
print("====================================")

hvac_indexer = StringIndexer(
    inputCol="HVACSystem",
    outputCol="HVACSystem_index",
    handleInvalid="keep"
)

hvac_encoder = OneHotEncoder(
    inputCol="HVACSystem_index",
    outputCol="HVACSystem_ohe"
)


# =========================================================
# 8. Apply Encoders
# =========================================================

ml_df = building_indexer.fit(
    df
).transform(
    df
)

ml_df = hvac_indexer.fit(
    ml_df
).transform(
    ml_df
)

ml_df = building_encoder.fit(
    ml_df
).transform(
    ml_df
)

ml_df = hvac_encoder.fit(
    ml_df
).transform(
    ml_df
)


# =========================================================
# 9. Prepare Features
# =========================================================

print("\n====================================")
print("Preparing ML Features")
print("====================================")

# IMPORTANT:
#
# EnergyPerArea is NOT included.
#
# Because:
#
# EnergyPerArea = EnergyConsumption / RoomArea
#
# EnergyConsumption is our target.
#
# Including EnergyPerArea would cause data leakage.

assembler = VectorAssembler(

    inputCols=[

        "AverageTemperature",

        # IMPORTANT:
        # ETL renamed:
        # Outside Temperature
        # ->
        # OutsideTemperature

        "OutsideTemperature",

        "InsulationThickness",

        "NumberofAppliances",

        "RoomArea",

        "TemperatureDifference",

        "BuildingType_ohe",

        "HVACSystem_ohe"

    ],

    outputCol="features",

    handleInvalid="skip"
)


ml_df = assembler.transform(
    ml_df
)


# =========================================================
# 10. Select ML Dataset
# =========================================================

ml_df = ml_df.select(

    "features",

    "EnergyConsumption",

    "BuildingType",

    "HVACSystem",

    "RoomArea",

    "NumberofAppliances",

    "InsulationThickness",

    "EnergyPerArea"
)


# Cache because we use the dataset multiple times

ml_df.cache()


print("\n====================================")
print("ML Dataset")
print("====================================")

ml_df.show(
    10,
    truncate=False
)

print(
    "\nML Record Count:",
    ml_df.count()
)


# =========================================================
# 11. Train / Test Split
# =========================================================

print("\n====================================")
print("Train / Test Split")
print("====================================")

train_data, test_data = ml_df.randomSplit(
    [0.8, 0.2],
    seed=42
)

train_count = train_data.count()
test_count = test_data.count()

print(
    "Training Records:",
    train_count
)

print(
    "Testing Records:",
    test_count
)


if train_count == 0 or test_count == 0:

    ml_df.unpersist()

    spark.stop()

    raise Exception(
        "Train or Test dataset is empty."
    )


# =========================================================
# 12. Linear Regression Model
# =========================================================

print("\n====================================")
print("Creating Linear Regression Model")
print("====================================")

lr = LinearRegression(

    featuresCol="features",

    labelCol="EnergyConsumption",

    maxIter=50,

    regParam=0.1,

    elasticNetParam=0.0
)


# =========================================================
# 13. Train Model
# =========================================================

print("\n====================================")
print("Training Linear Regression...")
print("====================================")

model = lr.fit(
    train_data
)

print(
    "Training Completed!"
)


# =========================================================
# 14. Predictions
# =========================================================

print("\n====================================")
print("Generating Predictions")
print("====================================")

predictions = model.transform(
    test_data
)


predictions.select(

    "EnergyConsumption",

    "prediction"

).show(
    10,
    truncate=False
)


# =========================================================
# 15. Building Health
# =========================================================

print("\n====================================")
print("Building Health Analysis")
print("====================================")


# Actual vs Expected

building_health = predictions.withColumn(

    "DeviationPercent",

    F.round(

        (
            F.col("EnergyConsumption")
            -
            F.col("prediction")
        )
        /
        F.when(
            F.col("prediction") != 0,
            F.col("prediction")
        ).otherwise(
            None
        )
        * 100,

        2
    )
)


# =========================================================
# 16. Calculate Efficiency Thresholds
# =========================================================

lower_cut, upper_cut = building_health.approxQuantile(

    "DeviationPercent",

    [0.10, 0.90],

    0.01

)


print(
    f"\nEfficiency thresholds:"
)

print(
    f"10th percentile: {lower_cut}%"
)

print(
    f"90th percentile: {upper_cut}%"
)


# =========================================================
# 17. Efficiency Status
# =========================================================

building_health = building_health.withColumn(

    "EfficiencyStatus",

    F.when(

        F.col("DeviationPercent")
        >=
        F.lit(upper_cut),

        "Inefficient (Above Expected)"

    )

    .when(

        F.col("DeviationPercent")
        <=
        F.lit(lower_cut),

        "Highly Efficient (Below Expected)"

    )

    .otherwise(
        "Normal Range"
    )
)


# =========================================================
# 18. Select Building Health Columns
# =========================================================

building_health = building_health.select(

    "BuildingType",

    "HVACSystem",

    "RoomArea",

    "NumberofAppliances",

    "InsulationThickness",

    "EnergyPerArea",

    F.col(
        "EnergyConsumption"
    ).alias(
        "ActualEnergy"
    ),

    F.round(
        F.col("prediction"),
        2
    ).alias(
        "ExpectedEnergy"
    ),

    "DeviationPercent",

    "EfficiencyStatus"
)


# =========================================================
# 19. Display Building Health
# =========================================================

print("\n====================================")
print("Building Health")
print("====================================")

building_health.orderBy(

    F.col("DeviationPercent").desc()

).show(
    10,
    truncate=False
)


# =========================================================
# 20. Count Flagged Records
# =========================================================

flagged_count = building_health.filter(

    F.col("EfficiencyStatus")
    !=
    "Normal Range"

).count()


total_count = building_health.count()


print(
    f"\nFlagged Records: "
    f"{flagged_count}"
)

print(
    f"Total Test Records: "
    f"{total_count}"
)


if total_count > 0:

    print(
        f"Flagged Percentage: "
        f"{flagged_count / total_count:.1%}"
    )


# =========================================================
# 21. Model Evaluation
# =========================================================

print("\n====================================")
print("Model Evaluation")
print("====================================")


rmse_evaluator = RegressionEvaluator(

    labelCol="EnergyConsumption",

    predictionCol="prediction",

    metricName="rmse"
)


mae_evaluator = RegressionEvaluator(

    labelCol="EnergyConsumption",

    predictionCol="prediction",

    metricName="mae"
)


r2_evaluator = RegressionEvaluator(

    labelCol="EnergyConsumption",

    predictionCol="prediction",

    metricName="r2"
)


rmse = rmse_evaluator.evaluate(
    predictions
)

mae = mae_evaluator.evaluate(
    predictions
)

r2 = r2_evaluator.evaluate(
    predictions
)


print(
    "RMSE:",
    rmse
)

print(
    "MAE :",
    mae
)

print(
    "R²  :",
    r2
)


# =========================================================
# 22. Save Predictions
# =========================================================

print("\n====================================")
print("Saving Predictions")
print("====================================")


predictions.select(

    "EnergyConsumption",

    "prediction",

    "BuildingType",

    "HVACSystem",

    "RoomArea",

    "NumberofAppliances",

    "InsulationThickness",

    "EnergyPerArea"

).write \
    .mode("overwrite") \
    .json(
        predictions_output
    )


print(
    "Predictions saved to:"
)

print(
    predictions_output
)


# =========================================================
# 23. Save Building Health
# =========================================================

print("\n====================================")
print("Saving Building Health")
print("====================================")


building_health.write \
    .mode("overwrite") \
    .json(
        building_health_output
    )


print(
    "Building health saved to:"
)

print(
    building_health_output
)


# =========================================================
# 24. Save Model
# =========================================================

print("\n====================================")
print("Saving Model")
print("====================================")


model.write() \
    .overwrite() \
    .save(
        model_output
    )


print(
    "Model saved to:"
)

print(
    model_output
)


# =========================================================
# 25. Final Summary
# =========================================================

print("\n====================================")
print("EnerWise ML SUMMARY")
print("====================================")

print(
    "Total Processed Records:",
    record_count
)

print(
    "Valid ML Records:",
    ml_record_count
)

print(
    "Training Records:",
    train_count
)

print(
    "Testing Records:",
    test_count
)

print(
    "RMSE:",
    rmse
)

print(
    "MAE:",
    mae
)

print(
    "R²:",
    r2
)

print(
    "Predictions:",
    predictions_output
)

print(
    "Building Health:",
    building_health_output
)

print(
    "Model:",
    model_output
)


# =========================================================
# 26. Stop Spark
# =========================================================

ml_df.unpersist()

spark.stop()


print("\n====================================")
print("EnerWise ML Completed Successfully")
print("====================================")