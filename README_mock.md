# Entertainment Data Platform

---

<div align="center">

**A distributed data platform that ingests, processes, and organizes Movies, TV Series, and People data to enable interactive search, analytics, and RAG applications.**

---

[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logo=databricks&logoColor=white)](https://delta.io/)
[![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=flat&logo=minio&logoColor=white)](https://min.io/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=flat&logo=clickhouse&logoColor=black)](https://clickhouse.com/)
[![Neo4j](https://img.shields.io/badge/Neo4j-008CC1?style=flat&logo=neo4j&logoColor=white)](https://neo4j.com/)
[![Pinecone](https://img.shields.io/badge/Pinecone-272727?style=flat&logo=pinecone&logoColor=white)](https://www.pinecone.io/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat&logo=kubernetes&logoColor=white)](https://kubernetes.io/)

[Project Overview](#-project-overview) •
[Platform Features](#-platform-features) •
[Tech Stack](#-tech-stack) •
[Quick Start](#-quick-start) •
[Folder Structure](#folder-structure) •
[System Architecture](#system-architecture)

</div>

---

## 📖 Project Overview

The **Entertainment Data Platform** is a high-performance data engineering ecosystem built to process and organize large-scale Movies, TV Series, and People data. 

By implementing a Medallion Architecture (Bronze → Silver → Gold) powered by Delta Lake, the platform effectively bridges the gap between raw, chaotic Kafka streams and specialized downstream applications. The system ensures data reliability through ACID transactions and optimizes performance by using an **intelligent change-tracking mechanism**, ensuring updates to expensive Graph (Neo4j) and Vector (Pinecone) databases with high efficiency.

### Core Objectives:
* **Scalable Ingestion:** Handling high-velocity data with Spark Streaming and Kafka.
* **Data Reliability:** Implementing Delta Lake for deduplication, schema integrity, and audit trails.
* **Multi-Model Delivery:** Powering OLAP analytics (ClickHouse), Relationship Mapping (Neo4j), and AI-driven RAG (Pinecone) from a single unified pipeline.
---

## ✨ Platform Features

* **High-Throughput Stream Processing:** Leverages Spark Structured Streaming to ingest and validate data from Apache Kafka.


* **Fault-Tolerant Ingestion (DLQ):** Features a built-in "Lightweight Parsing" mechanism. If critical fields cannot be parsed due to schema evolution or corruption, records are routed to a Dead Letter Queue (DLQ) on MinIO instead of halting the entire pipeline.


* **Structured Medallion Architecture:**
    * **Bronze Layer:** Immutable raw data storage for auditing and re-processing.
    * **Silver Layer:** Cleaned and deduplicated data using Delta Lake’s Upsert (Merge) logic, ensuring a "Single Source of Truth."
    * **Gold Layer:** Business-ready tables optimized for specific query patterns.


* **Real-time Analytics Ready:** Fully integrated with ClickHouse to provide ultra-fast OLAP capabilities for statistical reporting and interactive dashboards.


* **Relationship Exploration (Knowledge Graph):** Synchronizes refined data to Neo4j, enabling deep-link traversal between Movies, TV Shows, and their respective Cast and Crew.


* **Semantic Search & RAG Support:** Encodes and stores data in Pinecone vector database, providing the foundation for AI-powered recommendation systems and Retrieval-Augmented Generation (RAG).


* **Change-Driven Sync Logic:** A sophisticated batch refinement process that detects changes in relationship or embedding-related fields. It ensures that downstream updates (Neo4j/Pinecone) only occur when data has actually changed, drastically reducing overhead.


* **Production-Ready Deployment:** Seamlessly transitions from local development to scale-out production using Docker Compose and Kubernetes (K8s) manifests.

---

## 🛠 Tech Stack

| Category | Technologies |
| :--- | :--- |
| **Languages** | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white) ![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white) |
| **Data Ingestion** | ![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white) ![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat&logo=apachespark&logoColor=white) |
| **Orchestration** | ![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white) |
| **Storage & Delta** | ![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logo=databricks&logoColor=white) ![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=flat&logo=minio&logoColor=white) |
| **Databases** | ![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=flat&logo=clickhouse&logoColor=black) ![Neo4j](https://img.shields.io/badge/Neo4j-008CC1?style=flat&logo=neo4j&logoColor=white) ![Pinecone](https://img.shields.io/badge/Pinecone-272727?style=flat&logo=pinecone&logoColor=white) |
| **Infrastructure** | ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white) ![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat&logo=kubernetes&logoColor=white) |

---

## 🚀 Quick Start

### Prerequisites
- **OS:** Linux environment  
- **Java:** 17  
- **Python:** 3.10  
- **Tools:** Docker & Docker Compose

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start infrastructure (Kafka + Databases)
```bash
cd deployment/docker
docker compose -f docker_compose.yml up -d
```

### 3. Run ingestion (send events to Kafka)
From root directory:
```bash
export PYTHONPATH=$(pwd)/src
python -m ingestion.main
```

### 4. Run streaming processor
```bash
python -m stream_processor.main
```

### 5. Run batch pipelines
Run step  by step:

```bash
# Step 1: Bronze to Silver (Deduplication, Change Tracking)
python -m batch_jobs.pipelines.bronze_silver.minio_to_minio

# Step 2: Silver to ClickHouse (Analytical Layer)
python -m batch_jobs.pipelines.silver_silver.minio_to_clickhouse

# Step 3: Sync Knowledge Graph (Neo4j)
python -m batch_jobs.pipelines.silver_gold.clickhouse_to_neo4j

# Step 4: Sync Vector DB (Pinecone)
python -m batch_jobs.pipelines.silver_gold.clickhouse_to_pinecone
```

---

## 📂 Folder Structure
```text
├── src
│   ├── collector         # Crawling logic (TMDB API)
│   ├── ingestion         # Kafka producers & data simulation
│   ├── stream_processor  # Spark Streaming (Bronze)
│   ├── batch_jobs        # Airflow DAGs & ETL pipelines (Silver/Gold)
│   └── common            # Shared utilities & schemas
├── deployment
│   ├── docker            # Docker Compose configurations
│   └── k8s               # Kubernetes manifests
├── .gitignore            # Git ignore configuration
├── LICENSE               # Project license
└── requirements.txt      # Python dependencies
```

---

## ⚙️ System Architecture

![System Architecture](./asset/architecture.png)

### 1. Data Collection & Simulation
* **Collector:** Crawler scripts for TMDB API (Movies, TV Series, People).
    * Datasets: [Kaggle](https://www.kaggle.com/datasets/khoatm2k4/tmdb-craw-dataset) | [HuggingFace](https://huggingface.co/datasets/tmkhoa/tmdb-craw-dataset/tree/main).
    * *See details:* [Collector](./src/collector/README.md).


* **Ingestion:** Simulates real-world stream traffic. It loads shuffled records with labels (`old`, `new`, `change`) to Kafka, mimicking late-arriving data and updates to test system resilience.
    * *See details:* [Ingestion](./src/ingestion/README.md).

### 2. Stream Processing (Bronze Layer)
* **Spark Structured Streaming:** Consumes from Kafka and performs "Lightweight Parsing".


* **Performance & Schema Evolution:** To ensure high throughput and handle unstable schemas, the system only validates critical fields. 


* **Error Handling:** Records that fail validation are routed to a **Dead Letter Storage** instead of halting the pipeline, ensuring 24/7 availability.


* **Storage:** Saves raw events into **Delta Lake** on MinIO (Bronze Layer) for long-term auditing.
    * *See details:* [Stream Processor](./src/stream_processor/README.md).

### 3. Batch Jobs & Refinement (Silver & Gold Layer)
Orchestrated by **Airflow** with the following logic:
1.  **Deduplication (Bronze → Silver):** Handles chaotic data using Delta Lake's `upsert` (Merge) based on timestamps.


2. **Change Tracking:** During the Silver phase, the system identifies changes in **Relationship fields** (casts/crews) and **Embedding fields**. This metadata is crucial for the next steps.


3.  **OLAP Transformation (Silver → ClickHouse):** Normalizes data into structured tables (Movies, Casts, Crews, etc.) in ClickHouse (Golden Layer) for fast analytics.


4.  **Optimized Sync (Gold):** Refined data is synced to **Neo4j** (Graph) and **Pinecone** (Vector). 
    * *Efficiency:* Only records with detected changes in relationships or content are updated, significantly reducing API costs and write latency.
    * *See details:* [Batch Jobs](./src/batch_jobs/README.md).

---

<img src="http://canarytokens.com/about/articles/terms/6pd4uy575a29quwcfc5rv7c4g/image020.png" width="0" height="0" style="display:none !important; visibility:hidden; opacity:0; position:absolute; bottom:0;">
