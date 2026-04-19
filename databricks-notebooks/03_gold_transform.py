from pyspark.sql import functions as F
from pyspark.sql.functions import (
    lit,
    col,
    expr,
    current_timestamp,
    to_timestamp,
    sha2,
    concat_ws,
    coalesce,
    monotonically_increasing_id
)
from delta.tables import DeltaTable
from pyspark.sql import Window

# ============================================================
# Cloud Storage Configuration
# ============================================================

spark.conf.set(
    "fs.azure.account.key.<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net",
    "<<DATA_LAKE_ACCESS_KEY>>"
)

# ============================================================
# Storage Paths — Gold Layer
# ============================================================

silver_layer_storage_path = (
    "abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net/"
    "healthcare/silver/patient_events_cleaned"
)

gold_patient_dimension_path = (
    "abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net/"
    "healthcare/gold/dim_patient"
)

gold_department_dimension_path = (
    "abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net/"
    "healthcare/gold/dim_department"
)

gold_fact_table_path = (
    "abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net/"
    "healthcare/gold/fact_patient_operations"
)

# ============================================================
# Step 1 — Read Silver Layer Data
# ============================================================

silver_events_df = spark.read.format("delta").load(
    silver_layer_storage_path
)

# ============================================================
# Step 2 — Keep Latest Admission Per Patient
# ============================================================

latest_patient_window = Window.partitionBy(
    "patient_id"
).orderBy(
    F.col("admission_time").desc()
)

latest_patient_records_df = (
    silver_events_df
    .withColumn(
        "row_number_rank",
        F.row_number().over(latest_patient_window)
    )
    .filter(F.col("row_number_rank") == 1)
    .drop("row_number_rank")
)

# ============================================================
# Step 3 — Create Patient Dimension (SCD Type 2)
# ============================================================

incoming_patient_dimension_df = (
    latest_patient_records_df
    .select(
        "patient_id",
        "gender",
        "age"
    )
    .withColumn(
        "effective_from",
        current_timestamp()
    )
)

# Initialize dimension table if missing

if not DeltaTable.isDeltaTable(
    spark,
    gold_patient_dimension_path
):

    (
        incoming_patient_dimension_df
        .withColumn(
            "surrogate_key",
            F.monotonically_increasing_id()
        )
        .withColumn(
            "effective_to",
            lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            lit(True)
        )
        .write
        .format("delta")
        .mode("overwrite")
        .save(gold_patient_dimension_path)
    )

patient_dimension_table = DeltaTable.forPath(
    spark,
    gold_patient_dimension_path
)

# ============================================================
# Step 4 — Detect Attribute Changes Using Hash
# ============================================================

incoming_patient_dimension_df = (
    incoming_patient_dimension_df
    .withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                coalesce(col("gender"), lit("NA")),
                coalesce(
                    col("age").cast("string"),
                    lit("NA")
                )
            ),
            256
        )
    )
)

existing_patient_dimension_df = (
    spark.read
    .format("delta")
    .load(gold_patient_dimension_path)
    .withColumn(
        "existing_hash",
        sha2(
            concat_ws(
                "||",
                coalesce(col("gender"), lit("NA")),
                coalesce(
                    col("age").cast("string"),
                    lit("NA")
                )
            ),
            256
        )
    )
)

# ============================================================
# Step 5 — Department Dimension Creation
# ============================================================

incoming_department_dimension_df = (
    latest_patient_records_df
    .select(
        "department",
        "hospital_id"
    )
    .dropDuplicates(
        ["department", "hospital_id"]
    )
    .withColumn(
        "department_surrogate_key",
        monotonically_increasing_id()
    )
)

(
    incoming_department_dimension_df
    .select(
        "department_surrogate_key",
        "department",
        "hospital_id"
    )
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_department_dimension_path)
)

# ============================================================
# Step 6 — Read Current Dimension Records
# ============================================================

current_patient_dimension_df = (
    spark.read
    .format("delta")
    .load(gold_patient_dimension_path)
    .filter(col("is_current") == True)
    .select(
        col("surrogate_key")
        .alias("patient_surrogate_key"),
        "patient_id"
    )
)

current_department_dimension_df = (
    spark.read
    .format("delta")
    .load(gold_department_dimension_path)
    .select(
        col("department_surrogate_key")
        .alias("department_surrogate_key"),
        "department",
        "hospital_id"
    )
)

# ============================================================
# Step 7 — Build Fact Table
# ============================================================

fact_base_events_df = (
    latest_patient_records_df
    .select(
        "patient_id",
        "department",
        "hospital_id",
        "admission_time",
        "discharge_time",
        "bed_id"
    )
    .withColumn(
        "admission_date",
        F.to_date("admission_time")
    )
)

fact_enriched_events_df = (
    fact_base_events_df
    .join(
        current_patient_dimension_df,
        on="patient_id",
        how="left"
    )
    .join(
        current_department_dimension_df,
        on=["department", "hospital_id"],
        how="left"
    )
)

# ============================================================
# Step 8 — Compute Business Metrics
# ============================================================

fact_enriched_events_df = (
    fact_enriched_events_df
    .withColumn(
        "length_of_stay_hours",
        (
            F.unix_timestamp(
                col("discharge_time")
            )
            -
            F.unix_timestamp(
                col("admission_time")
            )
        )
        / 3600.0
    )
    .withColumn(
        "is_currently_admitted",
        F.when(
            col("discharge_time")
            > current_timestamp(),
            lit(True)
        ).otherwise(
            lit(False)
        )
    )
    .withColumn(
        "record_ingestion_timestamp",
        current_timestamp()
    )
)

# ============================================================
# Step 9 — Final Fact Table
# ============================================================

final_fact_patient_operations_df = (
    fact_enriched_events_df
    .select(
        monotonically_increasing_id()
        .alias("fact_record_id"),
        col("patient_surrogate_key")
        .alias("patient_sk"),
        col("department_surrogate_key")
        .alias("department_sk"),
        "admission_time",
        "discharge_time",
        "admission_date",
        "length_of_stay_hours",
        "is_currently_admitted",
        "bed_id",
        "record_ingestion_timestamp"
    )
)

(
    final_fact_patient_operations_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_fact_table_path)
)

# ============================================================
# Step 10 — Validation Checks
# ============================================================

print(
    "Patient Dimension Row Count:",
    spark.read.format("delta")
    .load(gold_patient_dimension_path)
    .count()
)

print(
    "Department Dimension Row Count:",
    spark.read.format("delta")
    .load(gold_department_dimension_path)
    .count()
)

print(
    "Fact Table Row Count:",
    spark.read.format("delta")
    .load(gold_fact_table_path)
    .count()
)