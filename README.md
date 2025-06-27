# Incremental-data-load-pipeline
This project demonstrates a robust and scalable Silver Layer transformation pipeline using Apache Spark and Delta Lake on Databricks
Project Overview:
This project demonstrates a robust and scalable Silver Layer transformation pipeline using Apache Spark and Delta Lake on Databricks. It implements incremental data loading, data quality enforcement, business rule transformations, and upserts into a Delta table optimized for downstream analytics. The pipeline follows the medallion architecture, processing raw Bronze-layer customer data into refined, query-optimized Silver-layer insights.

Core Components & Functionality:
1. Incremental Data Extraction from Bronze Layer
•	Extracts only new or updated records based on the Ingestion_time field by comparing with the last_updated timestamp from the Silver layer.
•	Efficiently handles data volume growth without full reloads.

2. Business Rule Application
Transforms Bronze records to Silver using defined business logic:
Email Validation: Excludes records with null email addresses.
Age Filter: Retains users aged between 18 and 30.
Customer Segmentation: Assigns value tiers:
High_value if total_purchases > 10,000
Medium_value if > 5,000
Else, Low_value
Days Since Registration: Computed using DATEDIFF function.
Quality Filter: Removes rows with negative purchase values.


3. Temporary Views for Staging
•	bronze_incremental: Holds filtered new records from Bronze.
•	silver_incremental: Holds cleaned and transformed records ready for merge.

4. Upsert Logic with Delta Merge
•	Uses Delta Lake's MERGE INTO operation to perform insert or update of records based on customer_id.
•	Ensures data consistency and avoids duplication.



Tools & Technologies:
Apache Spark (PySpark + SQL)
Delta Lake for ACID-compliant table storage
Databricks for notebook orchestration
SQL for data transformation logic

Business Impact:
Reduced processing time and infrastructure load by leveraging incremental ETL.
Increased data quality and compliance by applying validations and filtering.
Enabled advanced customer analytics through segmentation and recency tracking.
