# Anonymized Survey API to MySQL ETL Pipeline

**Automated Data Pipeline for Humanitarian Information Management**

## Overview

This project demonstrates an anonymized ETL pipeline that extracts structured survey data from multiple API endpoints, cleans and standardizes heterogeneous schemas, applies data quality checks and business rules, and loads the processed data into a structured MySQL database for reporting and analytics.

The pipeline was designed as a portfolio-safe sample to demonstrate practical information management, API-based data integration, data cleaning, deduplication, and database loading workflows relevant to humanitarian and development programme monitoring.

All sensitive fields, organization-specific references, personal identifiers, locations, and credentials have been removed, anonymized, or replaced with mock placeholders.

---

## Project Objectives

* Automate extraction of survey/programme data from multiple API endpoints.
* Standardize inconsistent field names and structures across different source forms.
* Clean, transform, and validate incoming data before database loading.
* Merge and deduplicate records across multiple sources.
* Retain the latest valid submission per unique record/entity.
* Apply workflow-specific business logic and derived calculations.
* Maintain structured MySQL tables for downstream analytics, dashboards, and reporting.
* Demonstrate a scalable approach for additional APIs, forms, or monitoring data sources.

---

## Tools and Technologies

* **Python** – ETL development and workflow automation
* **Pandas** – data cleaning, transformation, and schema harmonization
* **NumPy** – numerical calculations and missing-value handling
* **Requests** – API extraction and authenticated data retrieval
* **MySQL** – relational database storage
* **PyMySQL** – Python-to-MySQL database connection and batch loading
* **Environment Variables** – secure handling of credentials and configuration

---

## Pipeline Architecture

### 1. Data Extraction

The pipeline connects to multiple API endpoints and retrieves JSON-formatted survey/programme data. It includes basic response validation and error handling for missing, incomplete, or failed API responses.

Key features:

* Authenticated API requests.
* Extraction from multiple source forms/endpoints.
* Parallelized retrieval to improve processing efficiency.
* Validation of API response structure before processing.

---

### 2. Data Cleaning and Transformation

The extracted datasets are converted into structured dataframes and standardized before integration.

Key features:

* Column name normalization across different forms.
* Dynamic field merging where the same information may appear under different source columns.
* Standardization of dates, numeric values, categorical fields, and location/admin fields.
* Missing-value handling.
* Removal of duplicate records.
* Retention of the latest valid record based on submission timestamp.

---

### 3. Data Quality and Business Logic

The pipeline applies data quality rules and workflow-specific transformations before loading the data into MySQL.

Key features:

* Validation of required identifiers.
* Exclusion of incomplete or invalid records.
* Deduplication using unique record identifiers.
* Derived calculations for coverage periods, assistance values, and percentage-based metrics.
* Separation of records into logical output datasets according to workflow stage.
* Preparation of clean tables for reporting and dashboard use.

---

### 4. Data Integration

The pipeline creates structured datasets representing different operational domains, such as:

* Participant or household profile records.
* Service provider or partner-related records.
* Assistance or agreement records.
* Status update records.
* Approval or workflow completion records.
* Case closure or follow-up records.

These outputs are designed to support downstream analysis, programme monitoring, reporting, and dashboard development.

---

### 5. Database Loading

Processed datasets are inserted into MySQL using batch operations and upsert logic.

Key features:

* Batch insert/update operations.
* `ON DUPLICATE KEY UPDATE` logic to avoid duplicate database records.
* Incremental updates without overwriting unrelated data.
* Transaction handling with rollback in case of database errors.
* Structured database tables ready for reporting and analytics.

---

## Data Protection and Privacy

This repository is an anonymized portfolio sample.

* No personal data is included.
* No real API tokens, database credentials, or project identifiers are included.
* Credentials are handled through environment variables.
* Sensitive field names have been generalized.
* Organization-specific references have been removed.
* Sample fields and table names are illustrative and not linked to any real operational database.

---

## Example Workflow

1. Load configuration from environment variables.
2. Extract JSON data from multiple API endpoints.
3. Convert JSON responses into structured dataframes.
4. Normalize and harmonize fields across source datasets.
5. Apply cleaning, validation, deduplication, and derived calculations.
6. Create structured output tables by workflow domain.
7. Load cleaned records into MySQL using batch upsert operations.
8. Make the database available for reporting, dashboards, and monitoring analysis.

---

## Relevance to Information Management

This project demonstrates practical skills relevant to information management and monitoring systems, including:

* API-based data integration.
* Multi-source data consolidation.
* Data quality assurance.
* Data cleaning and validation.
* Database management.
* Workflow automation.
* Preparation of clean datasets for Power BI or other visualization tools.
* Support to evidence-based programme monitoring and decision-making.

---

## Repository Contents

```text
.
├── etl_pipeline.py          # Anonymized ETL script
├── requirements.txt         # Python dependencies
├── .env.example             # Example environment variable structure
├── README.md                # Project documentation
└── sample_schema.md         # Optional mock schema/documentation
```

---

## How to Use

1. Clone the repository.
2. Install dependencies from `requirements.txt`.
3. Create a `.env` file using `.env.example`.
4. Add approved API/database credentials locally.
5. Run the ETL script.
6. Review processed records in the MySQL database.

> Note: This repository is intended as a portfolio demonstration. It uses anonymized field names and mock configuration placeholders.

---

## Impact

The pipeline demonstrates how manual survey-data processing can be converted into a repeatable automated workflow. In a real operational setting, this type of pipeline can help:

* Reduce manual data preparation time.
* Improve consistency across reporting datasets.
* Strengthen data quality controls.
* Support faster dashboard updates.
* Improve access to reliable information for programme monitoring and decision-making.
* Provide a scalable foundation for additional forms, APIs, and reporting requirements.

---

## Author

**Osama Shabbar**
Information Management | Data Analysis | Python | SQL | Power BI

GitHub: https://github.com/Os-Shabbar
LinkedIn: https://www.linkedin.com/in/osama-shabbar/
