import json
import random
import uuid
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ============================================================
# Healthcare Event Streaming Configuration
# ============================================================

HEALTHCARE_EVENT_STREAM_NAMESPACE = "<<EVENT_STREAM_NAMESPACE>>"
HEALTHCARE_EVENT_TOPIC_NAME = "<<EVENT_STREAM_TOPIC>>"
HEALTHCARE_CONNECTION_STRING = "<<EVENT_STREAM_CONNECTION_STRING>>"


# ============================================================
# Initialize Kafka Producer for Healthcare Event Streaming
# ============================================================

healthcare_event_producer = KafkaProducer(
    bootstrap_servers=[
        f"{HEALTHCARE_EVENT_STREAM_NAMESPACE}:9093"
    ],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=HEALTHCARE_CONNECTION_STRING,
    value_serializer=lambda event: json.dumps(event).encode("utf-8")
)


# ============================================================
# Hospital Reference Data
# ============================================================

hospital_departments = [
    "Emergency",
    "Surgery",
    "ICU",
    "Pediatrics",
    "Maternity",
    "Oncology",
    "Cardiology"
]

patient_gender_categories = [
    "Male",
    "Female"
]


# ============================================================
# Function — Introduce Controlled Dirty Data (Data Quality Testing)
# ============================================================

def inject_data_quality_issues(patient_record):

    # 5% probability of invalid age
    if random.random() < 0.05:
        patient_record["age"] = random.randint(101, 150)

    # 5% probability of future admission timestamp
    if random.random() < 0.05:
        patient_record["admission_time"] = (
            datetime.utcnow()
            + timedelta(hours=random.randint(1, 72))
        ).isoformat()

    return patient_record


# ============================================================
# Function — Generate Patient Event
# ============================================================

def generate_healthcare_patient_event():

    admission_timestamp = (
        datetime.utcnow()
        - timedelta(hours=random.randint(0, 72))
    )

    discharge_timestamp = (
        admission_timestamp
        + timedelta(hours=random.randint(1, 72))
    )

    patient_event_payload = {
        "patient_id": str(uuid.uuid4()),
        "gender": random.choice(patient_gender_categories),
        "age": random.randint(1, 100),
        "department": random.choice(hospital_departments),
        "admission_time": admission_timestamp.isoformat(),
        "discharge_time": discharge_timestamp.isoformat(),
        "bed_id": random.randint(1, 500),
        "hospital_id": random.randint(1, 7)
    }

    return inject_data_quality_issues(patient_event_payload)


# ============================================================
# Main Streaming Loop
# ============================================================

if __name__ == "__main__":

    print("Starting Healthcare Event Streaming Producer...")

    while True:

        patient_event = generate_healthcare_patient_event()

        healthcare_event_producer.send(
            HEALTHCARE_EVENT_TOPIC_NAME,
            patient_event
        )

        print(
            f"Healthcare Event Sent: {patient_event}"
        )

        time.sleep(1)