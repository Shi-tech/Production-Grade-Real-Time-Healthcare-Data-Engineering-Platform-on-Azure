from pyspark.sql.types import *
from pyspark.sql.functions import *

# ============================================================
# Cloud Storage Configuration
# ============================================================

spark.conf.set(
    "fs.azure.account.key.<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net",
    "<<DATA_LAKE_ACCESS_KEY>>"
)

bronze_layer_storage_path = (
    "abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net/"
    "healthcare/bronze/patient_events"
)

silver_layer_storage_path = (
    "abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net/"
    "healthcare/silver/patient_events_cleaned"
)

silver_checkpoint_path = (
    "abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net/"
    "healthcare/checkpoints/silver/patient_events_cleaned"
)

# ============================================================
# Step 1 — Read Streaming Data from Bronze Layer
# ============================================================

bronze_stream_df = (
    spark.readStream
    .format("delta")
    .load(bronze_layer_storage_path)
)

# ============================================================
# Step 2 — Define Input Schema
# ============================================================

patient_event_schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])

# ============================================================
# Step 3 — Parse Raw JSON Payload
# ============================================================

parsed_patient_events_df = (
    bronze_stream_df
    .withColumn(
        "parsed_payload",
        from_json(col("healthcare_event_payload"), patient_event_schema)
    )
    .select("parsed_payload.*")
)

# ============================================================
# Step 4 — Standardize Timestamp Columns
# ============================================================

validated_patient_events_df = parsed_patient_events_df.withColumn(
    "admission_time",
    to_timestamp("admission_time")
)

validated_patient_events_df = validated_patient_events_df.withColumn(
    "discharge_time",
    to_timestamp("discharge_time")
)

# ============================================================
# Step 5 — Handle Invalid Admission Time Values
# ============================================================

validated_patient_events_df = validated_patient_events_df.withColumn(
    "admission_time",
    when(
        col("admission_time").isNull() |
        (col("admission_time") > current_timestamp()),
        current_timestamp()
    ).otherwise(col("admission_time"))
)

# ============================================================
# Step 6 — Handle Invalid Age Values
# ============================================================

validated_patient_events_df = validated_patient_events_df.withColumn(
    "age",
    when(
        col("age") > 100,
        floor(rand() * 90 + 1).cast("int")
    ).otherwise(col("age"))
)

# ============================================================
# Step 7 — Schema Alignment / Schema Evolution Support
# ============================================================

required_patient_columns = [
    "patient_id",
    "gender",
    "age",
    "department",
    "admission_time",
    "discharge_time",
    "bed_id",
    "hospital_id"
]

for required_column in required_patient_columns:
    if required_column not in validated_patient_events_df.columns:
        validated_patient_events_df = validated_patient_events_df.withColumn(
            required_column,
            lit(None)
        )

# ============================================================
# Step 8 — Write Cleaned Data to Silver Layer
# ============================================================

silver_stream_writer = (
    validated_patient_events_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", silver_checkpoint_path)
    .start(silver_layer_storage_path)
)