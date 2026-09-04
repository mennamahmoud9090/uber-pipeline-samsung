# Bronze Layer: Data Ingestion & Raw Storage

## 📌 Overview
The Bronze layer serves as the raw data repository for the Uber-like ride-sharing system. It handles the extraction of multi-format datasets and API responses, storing them in their original structure before any transformation occurs.

---

## 🗄️ Core Data Sources

1. **NYC Taxi & Limousine Commission (TLC) - High Volume Trip Records**
   * **Format:** Parquet & CSV
   * **Period:** December 2025 and January 2026.
   * **Purpose:** Provides large-scale historical ride records, pickup/drop-off locations, and trip metrics.
   * **Source URL:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

2. **Payments and Financial Transactions**
   * **Format:** JSON
   * **Key Fields:** `payment_id`, `trip_id`, `fare_amount`, `commission`, `surge_multiplier`, `payment_type`, and `is_refunded`.
   * **Purpose:** Captures financial transactions linked to each ride to compute profitability and surge pricing analytics.

3. **Open-Meteo Historical Weather Archive API**
   * **Format:** JSON Response via REST API
   * **Endpoint:** `https://archive-api.open-meteo.com/v1/archive?latitude=40.7128&longitude=-74.0060&start_date=2025-12-01&end_date=2026-01-31&hourly=temperature_2m,precipitation,snowfall&timezone=America/New_York`
   * **Parameters:** NYC coordinates (`40.7128`, `-74.0060`) tracking hourly temperature, precipitation, and snowfall during the two-month period.

---

## 🔄 Apache NiFi Ingestion Pipeline (`Finalll_Floww.xml`)

The data flow is orchestrated using Apache NiFi with two main pathways:

* **File Ingestion & Routing Flow (Upper Flow):**
  * **`GetFile`**: Fetches raw taxi trip datasets and payment files from the local shared directory.
  * **`RouteOnAttribute`**: Inspects file extensions and categorizes incoming streams into dedicated paths (`parquet_file`, `csv_file`, `json_file`).
  * **`PutHDFS`**: Writes the sorted raw files directly into the Hadoop Distributed File System (HDFS).

* **Weather API Ingestion Flow (Lower Flow):**
  * **`InvokeHTTP_Weather`**: Sends automated GET requests to the Open-Meteo archive endpoint to retrieve historical weather metrics.
  * **`PutHDFS`**: Stores the raw JSON weather payloads into HDFS for downstream processing.
