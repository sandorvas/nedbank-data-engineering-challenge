Nedbank Data Engineering Challenge

Medallion Architecture Pipeline (PySpark + Delta Lake)

⸻

🧭 Overview

This project implements a Medallion Architecture data pipeline using PySpark and Delta Lake, fully containerized with Docker.

The pipeline processes raw banking datasets into a structured analytical model:
	•	Bronze → Raw ingestion
	•	Silver → Cleaned and standardized data
	•	Gold → Dimensional model for analytics

⸻

🏗️ Architecture

Raw Data (CSV / JSONL)
        ↓
    Bronze Layer
    (Raw + ingestion timestamp)
        ↓
    Silver Layer
    (Cleaned + standardized)
        ↓
    Gold Layer
    (Dimensional Model)


⸻

🧱 Data Model (Gold Layer)

📊 fact_transactions
	•	transaction_sk (PK)
	•	account_sk (FK)
	•	customer_sk (FK)
	•	transaction_id
	•	transaction_date
	•	transaction_timestamp
	•	transaction_type
	•	merchant_category
	•	merchant_subcategory
	•	amount
	•	currency
	•	channel
	•	province
	•	dq_flag
	•	ingestion_timestamp

⸻

👤 dim_customers
	•	customer_sk (PK)
	•	customer_id
	•	gender
	•	province
	•	income_band
	•	segment
	•	risk_score
	•	kyc_status
	•	age_band

⸻

🏦 dim_accounts
	•	account_sk (PK)
	•	account_id
	•	customer_id
	•	account_type
	•	account_status
	•	open_date
	•	product_tier
	•	digital_channel
	•	credit_limit
	•	current_balance
	•	last_activity_date

⸻

⚙️ Key Design Decisions

✅ Medallion Architecture
	•	Clear separation of concerns
	•	Scalable and industry standard
	•	Supports incremental enhancements

⸻

✅ Deterministic Surrogate Keys
	•	Implemented using xxhash64
	•	Ensures:
	•	Stability across re-runs
	•	Idempotent pipeline behavior

⸻

✅ Config-Driven Pipeline
	•	No hardcoded paths
	•	Uses /data/config/pipeline_config.yaml
	•	Enables portability and reproducibility

⸻

✅ Delta Lake
	•	ACID transactions
	•	Schema enforcement
	•	Efficient storage (Parquet-based)

⸻

✅ Idempotency
	•	Pipeline can be re-run safely
	•	Outputs remain consistent for same inputs

⸻

🚀 How to Run

1. Build Docker Image

docker build -t nedbank-pipeline .


⸻

2. Run Pipeline

docker run \
  -v $(pwd)/../data:/data/input \
  -v $(pwd)/config:/data/config \
  -v $(pwd)/data/output:/data/output \
  nedbank-pipeline


⸻

📂 Project Structure

pipeline/
  ingest.py        → Bronze ingestion
  transform.py     → Silver transformation
  provision.py     → Gold layer (dimensions + fact)
  run_all.py       → Pipeline entrypoint

config/
  pipeline_config.yaml
  dq_rules.yaml

Dockerfile
requirements.txt


⸻

🧠 Assumptions & Trade-offs
	•	merchant_subcategory not present → set to NULL
	•	Age derived from DOB using year approximation
	•	Decimal casting applied for financial fields
	•	Inner joins used to ensure referential integrity

⸻

🧪 Data Quality
	•	Deduplication applied at each stage
	•	Referential integrity enforced in Gold layer
	•	Null foreign keys avoided (validated)

⸻

🏁 Summary

This solution demonstrates:
	•	End-to-end data pipeline design
	•	Strong data modeling practices
	•	Production-oriented engineering (Docker, config-driven)
	•	Clean and maintainable PySpark code

⸻

👤 Author

Sandor Vas
Data Engineer / Solution Architect
Johannesburg, South Africa
