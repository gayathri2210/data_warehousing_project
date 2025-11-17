End-to-End ETL Pipeline | PostgreSQL | Python | Medallion Architecture 

This project builds a complete data warehouse for reproductive health and fertility cycle analytics using the Medallion Architecture (Bronze → Silver → Gold).

It includes:

Automated ETL pipeline in Python
PostgreSQL data warehouse with star schema
Bronze → Silver → Gold transformations
Clean & organized tables for analytics

| Layer               | Tools                               |
| ------------------- | ----------------------------------- |
| **ETL**             | Python, Pandas                      |
| **Database**        | PostgreSQL, SQLAlchemy              |
| **Modeling**        | Star Schema, Medallion Architecture |

Features

1️⃣ Bronze Layer

Loads raw fertility cycle dataset (cycledata.csv)

Simple ingestion with basic cleaning

Stored as bronze_cycledata in PostgreSQL

2️⃣ Silver Layer

Cleaned & normalized analytical tables:

silver_cycle

silver_patient

silver_reproductivehistory

silver_symptoms

Fixes applied:

Stripped whitespace

Converted numeric columns

Converted blanks to NULL

Separated patient vs cycle vs reproductive history

3️⃣ Gold Layer (Star Schema)

Final analytical model:

🔷 Dimensions

dimpatient

dimcycle

🔶 Fact Table

factcycleanalytics

Contains:

Cycle metrics

Ovulation data

Luteal phase length

Bleeding intensity

Pregnancy history

