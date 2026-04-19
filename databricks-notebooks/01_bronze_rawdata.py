from pyspark.sql.functions import *

# ============================================================
# Healthcare Streaming Configuration
# ============================================================

# Event Streaming Service Configuration
healthcare_event_namespace = "<<EVENT_STREAM_NAMESPACE>>"
healthcare_event_topic = "<<EVENT_STREAM_TOPIC>>"
healthcare_event_connection = "<<EVENT_STREAM_CONNECTION_STRING>>"


streaming_kafka_config = {
    "kafka.bootstrap.servers": f"{healthcare_event_namespace}:9093",
    "subscribe": healthcare_event_topic,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f"""
        kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
        required username="$ConnectionString"
        password="{healthcare_event_connection}";
    """,
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}


# ============================================================
# Step 1 — Read Streaming Data from Event Service
# ============================================================

incoming_stream_df = (
    spark.readStream
    .format("kafka")
    .options(**streaming_kafka_config)
    .load()
)


# ============================================================
# Step 2 — Convert Raw Message to JSON Format
# ============================================================

healthcare_raw_json_df = incoming_stream_df.selectExpr(
    "CAST(value AS STRING) AS healthcare_event_payload"
)


# ============================================================
# Step 3 — Configure Cloud Storage Access (ADLS Gen2)
# ============================================================

spark.conf.set(
    "fs.azure.account.key.<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net",
    "<<DATA_LAKE_ACCESS_KEY>>"
)


# ============================================================
# Step 4 — Define Bronze Layer Storage Path
# ============================================================

bronze_layer_storage_path = (
    "abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.dfs.core.windows.net/"
    "healthcare/bronze/patient_events"
)


checkpoint_directory_path = (
    "dbfs:/mnt/healthcare/bronze/checkpoints/patient_event_stream"
)


# ============================================================
# Step 5 — Write Streaming Data to Bronze Layer (Delta Format)
# ============================================================

bronze_stream_writer = (
    healthcare_raw_json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_directory_path)
    .start(bronze_layer_storage_path)
)