# Uber-like System: Ride-Sharing Payments & Financial Metrics Analytics Pipeline

## 🚀 Project Overview
An end-to-end Data Engineering and Analytics solution designed for a ride-sharing (Uber-like) platform. This project implements the **Medallion Architecture (Bronze, Silver, Gold)** to ingest ride-sharing transactions and weather data, process and transform financial metrics using **Apache PySpark**, store them in **HDFS**, and load them into **Apache Hive** following a **Star Schema** to calculate profitability indicators like Net Revenue and Surge Multipliers.

---

## 🏗️ Architecture & Tech Stack
* **Data Ingestion:** Apache NiFi (Batch & API integration for CSV, Parquet, JSON)
* **Storage Layer (Data Lake):** HDFS (Hadoop Distributed File System)
* **Processing & ETL:** Apache PySpark & Spark SQL (Data Type Casting, Joins, Net Revenue Calculation)
* **Data Warehousing:** Apache Hive (External Tables, Star Schema)
* **Visualization & Reporting:** Power BI / Financial P&L Analytics Dashboards

---

## 📂 Medallion Architecture & Repository Structure
The project is structured following core data engineering layers:
* **`bronze/`**: Raw data ingestion logic, NiFi flow XML exports, and staging files (Payments, Rides Geo, Weather).
* **`silver/`**: PySpark ETL scripts for data cleaning, type casting, deduplication, and joining ride-sharing data with payments (`Staging_Payments_Joined`) to calculate metrics like `net_revenue` and handle surge logic.
* **`gold/`**: Business-ready aggregated data, Hive SQL queries, and Data Modeling (Star Schema definitions) for KPIs like Average Fare per Trip.

---

## 🔄 Pipeline Workflow
1. **Ingestion (Bronze):** NiFi extracts local datasets (CSV, Parquet, JSON) and queries the Weather API, routing raw payloads securely to HDFS.
2. **Transformation (Silver):** PySpark handles data type standardization, cleans missing values, and processes financial calculations (`net_revenue = fare_amount - commission`) alongside surge multiplier analysis.
3. **Warehousing & Analytics (Gold):** Processed Parquet files are loaded into Hive tables, powering final KPI aggregations (e.g., Avg. Fare per Trip and profitability analytics).

---

## 📊 System Visuals & Screenshots

### 1. Apache NiFi Ingestion Pipeline
![NiFi Pipeline](screenshots/nifi_pipeline.png)

### 2. Data Modeling & Star Schema
![Data Modeling](screenshots/data_modeling.png)

---

## 👥 Project Team
* **Mennatullah** (Data Ingestion from Sources)
* **Jana** (Data Transformation )
* **Basel** ( Data Warehousing)
* **Bassant** (Data Analysis )
