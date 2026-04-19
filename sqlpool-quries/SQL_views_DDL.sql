-- ============================================================
-- Healthcare KPI Views
-- ============================================================

-- 1. Bed Occupancy Percentage
CREATE VIEW vw_healthcare_bed_occupancy AS
SELECT 
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.bed_id END) * 1.0
        / COUNT(f.bed_id) * 100 AS bed_occupancy_percent
FROM dbo.fact_patient_operations f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;


-- 2. Bed Turnover Rate
CREATE VIEW vw_healthcare_bed_turnover_rate AS
SELECT 
    p.gender,
    COUNT(DISTINCT f.fact_record_id) * 1.0
        / COUNT(DISTINCT f.bed_id) AS bed_turnover_rate
FROM dbo.fact_patient_operations f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;


-- 3. Total Active Patients
CREATE VIEW vw_healthcare_patient_demographics AS
SELECT 
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.fact_record_id END) AS total_patients
FROM dbo.fact_patient_operations f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY p.gender;


-- 4. Average Treatment Duration
CREATE VIEW vw_healthcare_avg_treatment_duration AS
SELECT 
    d.department,
    p.gender,
    AVG(f.length_of_stay_hours) AS avg_treatment_duration
FROM dbo.fact_patient_operations f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;


-- ============================================================
-- Healthcare Reporting / Chart Views
-- ============================================================

-- 1. Patient Volume Trend Over Time
CREATE VIEW vw_healthcare_patient_volume_trend AS
SELECT 
    f.admission_date,
    p.gender,
    COUNT(DISTINCT f.fact_record_id) AS patient_count
FROM dbo.fact_patient_operations f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
GROUP BY f.admission_date, p.gender;


-- 2. Department-Wise Patient Inflow
CREATE VIEW vw_healthcare_department_inflow AS
SELECT 
    d.department,
    p.gender,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN f.fact_record_id END) AS patient_count
FROM dbo.fact_patient_operations f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;


-- 3. Overstay Patients Count
CREATE VIEW vw_healthcare_overstay_patients AS
SELECT 
    d.department,
    p.gender,
    COUNT(f.fact_record_id) AS overstay_count
FROM dbo.fact_patient_operations f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
WHERE f.length_of_stay_hours > 50
GROUP BY d.department, p.gender;


-- 4. Average Treatment Duration by Department and Gender
CREATE VIEW vw_healthcare_treatment_duration_summary AS
SELECT 
    d.department,
    p.gender,
    AVG(f.length_of_stay_hours) AS avg_treatment_duration
FROM dbo.fact_patient_operations f
JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
GROUP BY d.department, p.gender;