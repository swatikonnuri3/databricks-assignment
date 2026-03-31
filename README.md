# Databricks Assignment

A PySpark-based data engineering project built on Databricks, covering medallion architecture (Bronze → Silver → Gold) and REST API ingestion.

---

## 📁 Folder Structure

```
databricks-assignment/
├── src/
│   ├── Question_1/
│   │   ├── source_to_bronze/
│   │   │   ├── utils                        # Common/reusable functions (UDFs, schema helpers)
│   │   │   └── employee_source_to_bronze    # Driver notebook: reads CSVs, writes to Bronze
│   │   ├── bronze_to_silver/
│   │   │   └── employee_bronze_to_silver    # Reads Bronze, applies transformations, writes to Silver
│   │   └── silver_to_gold/
│   │       └── employee_silver_to_gold      # Reads Silver, aggregates, writes to Gold
│   └── Question_2/
│       └── question2                        # API ingestion, flattening, writes to Delta table
└── README.md
```

---

##  Assignment Overview

### Question 1 — Medallion Architecture (Employee Data)

**Datasets:** Employee, Department, Country (CSV files)

####  Source to Bronze — `employee_source_to_bronze`
- Reads 3 CSV datasets as DataFrames
- Calls `utils` notebook for common functions
- Writes raw files to DBFS:
  - `/source_to_bronze/employee.csv`
  - `/source_to_bronze/department_df.csv`
  - `/source_to_bronze/country_df.csv`

#### Bronze to Silver — `employee_bronze_to_silver`
- Calls `utils` notebook
- Reads CSV files from `/source_to_bronze/` using custom schema
- Converts **camelCase** column names to **snake_case** using a UDF
- Adds `load_date` column with current date
- Writes as **Delta table**:
  - Path: `/silver/Employee_info/dim_employee`
  - Primary Key: `EmployeeID`
  - DB: `Employee_info` | Table: `dim_employee`

#### Silver to Gold — `employee_silver_to_gold`
- Calls `utils` notebook
- Reads Silver Delta table as DataFrame
- Produces the following analytical outputs:
  1. Salary of each department in **descending order**
  2. Number of employees per department per country
  3. Department names with corresponding country names
  4. Average age of employees per department
- Adds `at_load_date` column to each output DataFrame
- Writes to DBFS with **overwrite** mode and `replaceWhere` on `at_load_date`:
  - Path: `/gold/employee/fact_employee`

---

### Question 2 — REST API Ingestion (User Data)

**API:** `https://dummyjson.com/users` *(adapted from reqres.in)*

- Fetches paginated data using `skip` & `limit` params, looping until response is empty
- Drops top-level metadata columns: `total`, `skip`, `limit`
- Reads response into a DataFrame with a **custom schema**
- **Flattens** all nested structs (address, bank, company)
- Derives `site_address` column from `email` (extracts domain → `dummyjson.com`)
- Adds `load_date` with current date
- Writes as **managed Delta table**:
  - DB: `site_info` | Table: `person_info`
  - Mode: `overwrite`

---

##  Technologies Used

| Tool | Purpose |
|---|---|
| Databricks (Serverless) | Notebook execution environment |
| Apache Spark (PySpark) | Distributed data processing |
| Delta Lake | Storage format for Silver & Gold layers |
| Python `requests` | REST API data ingestion |
| Unity Catalog | Managed table storage |
| GitHub | Version control |

---

## Setup

1. Clone the repo into Databricks via **Repos → Add Repo**
2. Link your GitHub credentials in **User Settings → Git Integration**
3. Upload the source CSV files (Employee, Department, Country) to the workspace
4. Run notebooks in this order:
   ```
   utils → employee_source_to_bronze → employee_bronze_to_silver → employee_silver_to_gold
   ```
5. For Question 2, run `question2` notebook independently
