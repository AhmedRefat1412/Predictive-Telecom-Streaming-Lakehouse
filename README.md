# 📡 Predictive Telecom Streaming Lakehouse

An end-to-end **real-time Data Engineering platform** that simulates telecom tower monitoring using a modern streaming lakehouse architecture with live anomaly detection and alerting.

---

## 📌 Project Overview

This project demonstrates how to build a **production-grade streaming data platform** powered by simulated APIs.

The system generates real-time telecom telemetry data and processes it to:

* Monitor system behavior continuously
* Detect anomalies using Machine Learning
* Trigger real-time alerts
* Enable live dashboarding

The architecture follows:

* **Streaming-first architecture**
* **Medallion Architecture (Bronze → Silver → Gold)**
* **Lakehouse design using Apache Iceberg**

---

## 🏗️ Architecture

![Architecture](https://github.com/AhmedRefat1412/Predictive-Telecom-Streaming-Lakehouse/blob/main/telecom_streem.png)

---

## ⚡ Streaming Pipeline (Core Flow)

### 📡 Data Simulation (APIs)

* 4 simulated APIs generating real-time telecom data:
  - System metrics
  - Network data
  - Radio signals
  - Environmental data

---

###  Pipeline Flow

1. **Ingestion (Kafka)**
   - Data is ingested from APIs into Kafka topics

2. **Stream Processing (PySpark)**
   - Spark Structured Streaming consumes Kafka streams
   - Each data stream is processed independently

3. **Silver Layer (S3 + Iceberg)**
   - Data cleaned and transformed
   - Streams unified (join بين كل الـ units)
   - Stored in **Parquet format**
   - Managed using **Apache Iceberg tables**

4. **Gold Layer + ML**
   - Isolation Forest model applied on streaming data
   - New column added → anomaly prediction
   - Data enriched and analytics-ready

5. **Query Layer**
   - Data cataloged using AWS Glue
   - Queried via AWS Athena

6. **Visualization**
   - Grafana dashboard reads from Gold layer

---

## 🧠 Machine Learning (Streaming)

* Model: **Isolation Forest**
* Applied on each micro-batch in real-time

### 🎯 Purpose:

* Detect abnormal telecom behavior
* Predict potential failures

---

## 🔔 Real-Time Alerting (Independent Pipeline)

To ensure low-latency alerting:

* Separate Kafka-based pipeline
* Python microservice consumes streaming data
* Custom conditions applied
* Sends alerts via **Telegram Bot**

### ✅ لماذا Pipeline منفصل؟

* تقليل latency
* عدم الاعتماد على Spark micro-batch delay
* Alerting في شبه real-time حقيقي

---

## 🏗️ Lakehouse Architecture

### 🥉 Bronze Layer

* Raw streaming data from Kafka
* Stored in S3 (Parquet)
* No transformation

---

### 🥈 Silver Layer

* Data cleaning + transformation
* Join لكل الـ streams في record موحد
* Stored as Iceberg tables

---

### 🥇 Gold Layer

* Data enriched with ML predictions
* Optimized for analytics
* Used by Grafana

---

##  Apache Iceberg (Key Feature)

Used to solve streaming storage challenges:

### Problem:

* Streaming generates many small files on S3

###  Solution:

* Iceberg manages:
  - Compaction
  - Re-partitioning
  - ACID transactions

### Background Jobs:

* Scheduled compaction job
* Optimizes data layout periodically

---

## ⚙️ Orchestration (Apache Airflow)

Two main DAGs:

### 1-Iceberg Maintenance DAG

* Compaction
* Data optimization (Bronze / Silver / Gold)

### 2- ML Training DAG

* Retrains Isolation Forest model
* Uses historical data from lakehouse

---

## 🔴 Real-Time Dashboard (Grafana)

![Grafana Dashboard](https://github.com/AhmedRefat1412/Predictive-Telecom-Streaming-Lakehouse/blob/main/dashboar_telecom).png)

* Live monitoring of telecom metrics
* Displays anomaly signals
* Connected to Athena / Gold layer

---

## 🗄️ Storage & Query Layer

* **Amazon S3** → Data Lake
* **Apache Iceberg** → Table format
* **AWS Glue** → Metadata Catalog
* **AWS Athena** → Query Engine

---

## 🛠️ Tech Stack

* Apache Kafka
* PySpark Structured Streaming
* Apache Iceberg
* AWS S3
* AWS Glue
* AWS Athena
* Apache Airflow
* Scikit-learn (Isolation Forest)
* Grafana
* Docker
* Telegram Bot API
* Python
