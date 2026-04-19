-- ============================================================
-- Step 1 — Create Database Master Key
-- ============================================================

CREATE MASTER KEY
ENCRYPTION BY PASSWORD = '<<STRONG_DATABASE_PASSWORD>>';


-- ============================================================
-- Step 2 — Create Database Scoped Credential
-- ============================================================

CREATE DATABASE SCOPED CREDENTIAL healthcare_storage_credential
WITH
IDENTITY = 'Managed Identity';


-- ============================================================
-- Step 3 — Create External Data Source
-- ============================================================

CREATE EXTERNAL DATA SOURCE healthcare_gold_data_source
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://<<DATA_CONTAINER>>@<<DATA_LAKE_ACCOUNT>>.core.windows.net/',
    CREDENTIAL = healthcare_storage_credential
);


-- ============================================================
-- Step 4 — Define External File Format
-- ============================================================

CREATE EXTERNAL FILE FORMAT parquet_file_format_healthcare
WITH (
    FORMAT_TYPE = PARQUET
);


-- ============================================================
-- Step 5 — Patient Dimension Table
-- ============================================================

CREATE EXTERNAL TABLE dbo.dim_patient (
    patient_id VARCHAR(50),
    gender VARCHAR(10),
    age INT,
    effective_from DATETIME2,
    surrogate_key BIGINT,
    effective_to DATETIME2,
    is_current BIT
)
WITH (
    LOCATION = 'healthcare/gold/dim_patient/',
    DATA_SOURCE = healthcare_gold_data_source,
    FILE_FORMAT = parquet_file_format_healthcare
);


-- ============================================================
-- Step 6 — Department Dimension Table
-- ============================================================

CREATE EXTERNAL TABLE dbo.dim_department (
    surrogate_key BIGINT,
    department NVARCHAR(200),
    hospital_id INT
)
WITH (
    LOCATION = 'healthcare/gold/dim_department/',
    DATA_SOURCE = healthcare_gold_data_source,
    FILE_FORMAT = parquet_file_format_healthcare
);


-- ============================================================
-- Step 7 — Fact Table
-- ============================================================

CREATE EXTERNAL TABLE dbo.fact_patient_operations (
    fact_record_id BIGINT,
    patient_sk BIGINT,
    department_sk BIGINT,
    admission_time DATETIME2,
    discharge_time DATETIME2,
    admission_date DATE,
    length_of_stay_hours FLOAT,
    is_currently_admitted BIT,
    bed_id INT,
    record_ingestion_timestamp DATETIME2
)
WITH (
    LOCATION = 'healthcare/gold/fact_patient_operations/',
    DATA_SOURCE = healthcare_gold_data_source,
    FILE_FORMAT = parquet_file_format_healthcare
);


-- ============================================================
-- Step 8 — Validation Query
-- ============================================================

SELECT *
FROM dbo.fact_patient_operations;