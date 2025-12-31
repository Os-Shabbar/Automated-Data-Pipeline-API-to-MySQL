# Automated-Data-Pipeline-API-to-MySQL

## Overview
Automated data extraction, transformation, and loading (ETL) of API data into database.

---

## Project Objectives
- Automate data extraction from multiple APIs 
- Clean and standardize heterogeneous survey schemas  
- Merge and deduplicate records across multiple sources  
- Apply complex business logic for complex workflows
- Maintain a structured MySQL database for downstream analytics  

---

## Tools
- **Python**
- **Pandas, NumPy** – data cleaning and transformation
- **Requests** – API integration
- **MySQL & PyMySQL** – relational database
- **API**

---

## Pipeline Architecture

### 1. Data Extraction
- Authenticated API requests to multiple endpoints
- Retrieval of JSON data from multiple sources
- Error handling for missing or incomplete responses

### 2. Data Cleaning & Transformation
- Column normalization across datasets
- Dynamic merging of relevant fields
- Consolidation of identifiers across sources
- Handling missing values, duplicates, and inconsistent formats
- Conversion of date, numeric, and geolocation fields

### 3. Business Logic
- Separation of workflows for different data streams
- Calculation of derived metrics (e.g., coverage periods, percentages, aggregations)
- Retention of the latest valid submission per entity

### 4. Data Integration
- Creation of structured datasets for multiple domains, such as:
  - Beneficiary / user data
  - Agreements / contracts
  - Landlord / property data
  - Signatures and approvals
  - Cancellation / follow-up records
  

### 5. Database Loading
- Insert/update operations using `ON DUPLICATE KEY UPDATE`
- Incremental updates without data loss
- Referential consistency across tables


---

## Impact
- Reduced manual data processing
- Improved data accuracy and consistency
- Enabled timely, reliable reporting for stakeholders
- Scalable design for additional APIs or data source

---

## Data Security & Privacy
- API tokens and credentials are **not hard-coded**
- Public repository uses placeholders and anonymized fields
- No personally identifiable information (PII) is exposed

---

## Use Cases
- Data Analyst / Information Management roles
- ETL pipeline demonstrations
- API-based data integration projects
- Scalable data solutions for operational or humanitarian programs

---

## Author
**Osama Shabbar**  
Data Analyst | Python | SQL | Power BI  
GitHub: https://github.com/Os-Shabbar  
LinkedIn: https://www.linkedin.com/in/osama-shabbar/
