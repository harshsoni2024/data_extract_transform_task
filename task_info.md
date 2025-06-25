Design and implement a small-scale yet fully functional data warehouse that includes the following core components:

Data Ingestion – Develop pipelines to extract and load data from multiple sources, such as APIs, relational databases, and flat files (e.g., CSV, JSON). This ensures a diverse and realistic dataset.

Slowly Changing Dimensions (SCDs) – Implement techniques to handle changing dimensional data, ensuring historical accuracy. Choose between SCD Type 1 (overwrite), Type 2 (track changes with versioning), or other variations based on the use case.

Star Schema Modeling – Structure the data warehouse using a fact table for transactional data and dimension tables for descriptive attributes, optimizing for analytical queries.

Incremental Data Loading – Design ETL (Extract, Transform, Load) processes that only process new or changed data instead of reloading everything. This improves efficiency and reduces processing costs.

Basic Reporting & Analytics – Build simple reporting features (e.g., SQL queries, dashboards) to demonstrate the warehouse’s ability to support data-driven insights.