# Silver Layer: Data Cleansing & PySpark ETL

This folder contains the PySpark scripts responsible for data transformation and cleaning:
* Data type standardization, handling missing values, and deduplication.
* Joining ride-sharing data with payment records (`Staging_Payments_Joined`).
* Calculating profitability metrics such as `net_revenue` (`fare_amount - commission`) and handling surge pricing logic (`surge_multiplier`).
