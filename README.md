# ☁️ Azure Data Engineering Project — AdventureWorks Lakehouse
Azure Data Engineering End-To-End Project | Azure Data Factory | Databricks | Pyspark | Azure Synapse Analytics

[![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/products/data-factory)
[![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://azure.microsoft.com/products/databricks)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Azure Synapse](https://img.shields.io/badge/Azure%20Synapse%20Analytics-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/products/synapse-analytics)
[![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-0089D6?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/products/storage/data-lake-storage)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

## 📋 Table of Contents
- [Overview](#-overview)
- [Architecture](#-architecture)
- [Technologies](#-technologies)
- [Project Structure](#-project-structure)
- [Data Pipeline Flow](#-data-pipeline-flow)
- [Layer Implementation](#-layer-implementation)
- [Key Features](#-key-features)
- [Setup & Deployment](#-setup--deployment)
- [Use Cases](#-use-cases)
- [Dataset](#-dataset)
- [Roadmap & Known Limitations](#-roadmap--known-limitations)

---

## 🎯 Overview

This project implements a **metadata-driven, layered lakehouse pipeline on Azure** for the AdventureWorks retail dataset. Rather than hand-building a separate pipeline for every source file, ingestion is fully **parameterized and config-driven**: a single Azure Data Factory pipeline reads a JSON control table and dynamically ingests every dataset it describes. From there, the data flows through a classic Bronze → Silver → Gold lakehouse, transformed with **PySpark on Databricks** and served for analytics through **Azure Synapse Analytics** serverless SQL.

It demonstrates production data engineering patterns including:

- ✅ **Metadata-driven ingestion** — one reusable ADF pipeline (`Lookup` + `ForEach` + `Copy`) replacing what would otherwise be a hardcoded pipeline per source file
- ✅ **HTTP-to-Lake ingestion** — pulling CSVs directly from GitHub raw URLs into ADLS Gen2, no manual file staging
- ✅ **PySpark transformations on Databricks** — type casting, derived columns, string parsing, and Parquet conversion
- ✅ **Serverless SQL serving layer** — Synapse `OPENROWSET` views and external tables over Parquet, secured with a **Managed Identity** credential rather than embedded keys
- ✅ **Infrastructure as exportable ARM/JSON artifacts** — every ADF pipeline, dataset, and linked service is version-controlled as JSON, not just clicked together in a portal

### Business Problem

Retail analytics on a dataset like AdventureWorks (customers, products, sales, returns, territories, calendar) requires data to move reliably from a source system into a governed, queryable serving layer. This pipeline shows how to do that on Azure **without duplicating pipeline logic per source** — adding an eleventh dataset is a one-line addition to a control file, not a new pipeline.

---

## 🏗️ Architecture

<img width="1211" height="481" alt="Azure-Data-Engineering-Project-architecture" src="https://github.com/user-attachments/assets/a7ee9400-a47e-4e4a-92ed-fb8cd214b49c" />


```text
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────┐
│              │     │                   │     │                   │     │                   │     │                   │     │              │
│ HTTP SOURCE  │────▶│  DATA INGESTION   │────▶│   BRONZE LAYER    │────▶│   SILVER LAYER    │────▶│    GOLD LAYER     │────▶│  REPORTING   │
│              │     │                   │     │                   │     │                   │     │                   │     │              │
│  GitHub raw  │     │ Azure Data        │     │ ADLS Gen2 —       │     │ Databricks/       │     │ Azure Synapse —   │     │  Power BI    │
│  CSV files   │     │ Factory           │     │ raw data store    │     │ PySpark —         │     │ serverless SQL    │     │              │
│  (10 files)  │     │ (metadata-driven) │     │                   │     │ transformed data  │     │ views + ext tables│     │              │
└──────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────┘
```

**How to read the diagram**: 10 AdventureWorks CSV files are pulled from a GitHub raw HTTP endpoint by Azure Data Factory into an ADLS Gen2 **Bronze** container. Databricks reads the raw CSVs with PySpark, applies cleansing/derived-column logic, and writes Parquet into an ADLS Gen2 **Silver** container. Azure Synapse Analytics exposes that Silver Parquet data as **Gold** views and external tables via serverless SQL, ready for **Power BI** to consume.

---

## 🛠️ Technologies

| Category | Technology |
|---|---|
| Orchestration / Ingestion | Azure Data Factory (Lookup, ForEach, Copy activities) |
| Storage | Azure Data Lake Storage Gen2 (Bronze + Silver containers) |
| Transformation | Azure Databricks, PySpark |
| Serving / Warehousing | Azure Synapse Analytics (serverless SQL pool, external tables, `OPENROWSET`) |
| File Format | Parquet (Snappy compression), DelimitedText (CSV source) |
| Security | Managed Identity (database-scoped credential), Azure Blob FS encrypted linked service credentials |
| Reporting | Power BI (architecture target; consumes the Synapse serving layer) |
| Source | AdventureWorks sample dataset, hosted as CSVs on GitHub |

---

## 📂 Project Structure

```
Azure-Data-Engineering-Project/
├── adf/
│   ├── pipeline/
│   │   ├── GitToRaw.json              # single-file hardcoded copy (products.csv only)
│   │   └── DynamicGitToRaw.json       # metadata-driven multi-file ingestion
│   ├── dataset/
│   │   ├── ds_git_parameters.json     # reads git.json control file from ADLS
│   │   ├── ds_git_dynamic.json        # parameterized HTTP source dataset
│   │   ├── ds_sink_dynamic.json       # parameterized ADLS bronze sink dataset
│   │   ├── ds_http.json               # static HTTP source (products.csv)
│   │   └── ds_bronze.json             # static bronze sink (products.csv)
│   └── linkedService/
│       ├── httplinkedservice.json     # GitHub raw endpoint (raw.githubusercontent.com)
│       └── storage.json               # ADLS Gen2 storage account connection
├── synapse/
│   └── sqlscipts/
│       ├── Create Schema.json         # CREATE SCHEMA gold
│       ├── Create external table.json # credential, external data sources, file format, ext table
│       ├── CREATE VIEWS GOLD.json     # 7 gold views over Silver Parquet via OPENROWSET
│       └── SQL script 1.json          # ad-hoc query example
├── (Clone) silver_layer (without credentials).ipynb   # PySpark Bronze → Silver transformation notebook
├── parameters/
│   └── git.json                       # ingestion control table (10 source/sink mappings)
├── holding/                           # archived exports: architecture diagram, factory/IR/credential
│   ├── azure_pipeline_architecture (1).drawio.png
│   └── datasets/                      # source AdventureWorks CSVs (10 files, ~77K rows)
└── README.md
```

---

## 🔄 Data Pipeline Flow

### 1️⃣ **Metadata-driven ingestion** — Azure Data Factory

**File**: `parameters/git.json` — the control table driving every ingestion run:

```json
[
  {
    "p_relative_url": "jiahern06/Azure-Data-Engineering-Project/refs/heads/main/datasets/AdventureWorks_Calendar.csv",
    "p_sink_folder": "Calendar",
    "p_sink_file": "calendar.csv"
  },
  {
    "p_relative_url": "jiahern06/Azure-Data-Engineering-Project/refs/heads/main/datasets/AdventureWorks_Sales_2017.csv",
    "p_sink_folder": "Sales_2017",
    "p_sink_file": "sales_2017.csv"
  }
  // ... 8 more entries (Customers, Categories, Subcategories, Products, Returns,
  //     Sales_2015, Sales_2016, Territories)
]
```

**File**: `adf/pipeline/DynamicGitToRaw.json` — the pipeline that consumes it:

```json
{
  "name": "LookupGit",
  "type": "Lookup",
  "typeProperties": {
    "source": { "type": "JsonSource", "storeSettings": { "type": "AzureBlobFSReadSettings" } },
    "dataset": { "referenceName": "ds_git_parameters", "type": "DatasetReference" }
  }
},
{
  "name": "ForEachGit",
  "type": "ForEach",
  "dependsOn": [{ "activity": "LookupGit", "dependencyConditions": ["Succeeded"] }],
  "typeProperties": {
    "items": { "value": "@activity('LookupGit').output.value", "type": "Expression" },
    "isSequential": true,
    "activities": [ /* DynamicCopy: HTTP GET -> ADLS BlobFS sink, per-item parameters */ ]
  }
}
```

`LookupGit` reads every row of `git.json`, then `ForEachGit` iterates the list and runs a `DynamicCopy` activity per row — pulling `p_relative_url` from GitHub raw over HTTP and landing it at `p_sink_folder`/`p_sink_file` in the ADLS `bronze` file system. **One pipeline, ten files, zero duplication.** The repo also keeps `GitToRaw.json` — the earlier single-file, hardcoded version — as a visible before/after of the design decision.

---

### 2️⃣ **Bronze Layer** — ADLS Gen2 raw landing zone

**File**: `adf/dataset/ds_sink_dynamic.json`

```json
{
  "name": "ds_sink_dynamic",
  "properties": {
    "linkedServiceName": { "referenceName": "storage", "type": "LinkedServiceReference" },
    "parameters": { "p_sink_folder": { "type": "String" }, "p_filename": { "type": "String" } },
    "type": "DelimitedText",
    "typeProperties": {
      "location": {
        "type": "AzureBlobFSLocation",
        "fileName": { "value": "@dataset().p_filename", "type": "Expression" },
        "folderPath": { "value": "@dataset().p_sink_folder", "type": "Expression" },
        "fileSystem": "bronze"
      },
      "firstRowAsHeader": true
    }
  }
}
```

Every one of the 10 source files lands in its own folder under the `bronze` file system in ADLS Gen2, exactly as named in `git.json` — no transformation yet, just a faithful raw copy.

---

### 3️⃣ **Bronze → Silver** — PySpark on Databricks

**File**: `(Clone) silver_layer (without credentials).ipynb`

Each entity is read from Bronze, given entity-specific cleansing/enrichment, and written to Silver as Parquet:

```python
# Calendar — derive Month/Year from Date
df_cal = spark.read.format('csv').option('header','true').option('InferSchema', True) \
    .load('abfss://bronze@awprojectstorage.dfs.core.windows.net/Calendar')
df_cal = df_cal.withColumn('Month', month(col('Date'))) \
               .withColumn('Year', year(col('Date')))
df_cal.write.format('parquet').mode('append') \
    .option('Path', 'abfss://silver@awprojectstorage.dfs.core.windows.net/Calendar').save()

# Customers — build a single fullName field
df_cust = df_cus.withColumn('fullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))

# Products — parse SKU/name tokens out of composite fields
df_prod = df_prod.withColumn("ProductSKU", split(col("ProductSKU"), '-')[0]) \
                  .withColumn("ProductName", split(col("ProductName"), ' ')[0])

# Sales — normalize order numbers, cast dates, derive a line-total helper column
df_sales = df_sales.withColumn('StockDate', to_timestamp('StockDate')) \
                    .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T')) \
                    .withColumn('multiple', col('OrderLineItem') * col('OrderQuantity'))
```

The three yearly sales files are read back in with a single wildcard load — `.load('.../Sales*')` — merging `Sales_2015`, `Sales_2016`, and `Sales_2017` into one Silver `Sales` dataset in the same pass.

---

### 4️⃣ **Silver → Gold** — Azure Synapse Analytics serverless SQL

**File**: `synapse/sqlscipts/Create external table.json`

```sql
CREATE DATABASE SCOPED CREDENTIAL cred_jiahern
WITH IDENTITY = 'Managed Identity'

CREATE EXTERNAL DATA SOURCE source_silver
WITH (LOCATION = 'https://awprojectstorage.blob.core.windows.net/silver/', CREDENTIAL = cred_jiahern)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH (FORMAT_TYPE = PARQUET, DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec')

CREATE EXTERNAL TABLE gold.extsales
WITH (LOCATION = 'exstsales', DATA_SOURCE = source_gold, FILE_FORMAT = format_parquet)
AS SELECT * FROM gold.sales
```

**File**: `synapse/sqlscipts/CREATE VIEWS GOLD.json`

```sql
CREATE VIEW gold.customer AS
SELECT * FROM OPENROWSET(
    BULK 'https://awprojectstorage.blob.core.windows.net/silver/Customers/',
    FORMAT = 'PARQUET'
) AS QUERY1
```

Gold-layer access is entirely **serverless** — no data is duplicated into Synapse dedicated pools. `OPENROWSET` views read Silver Parquet directly, secured through a **database-scoped credential backed by Managed Identity** (not a hardcoded key). Seven such views are created (`calendar`, `customer`, `products`, `returns`, `sales`, `subcat`, `territories`), and one — `gold.extsales` — is additionally materialized as a physical **external table** for faster repeat access.

---

## 🧩 Layer Implementation

| Layer | Technology | What happens |
|---|---|---|
| **Source** | GitHub (raw HTTP) | 10 AdventureWorks CSVs hosted as static files |
| **Ingestion** | Azure Data Factory | Metadata-driven `Lookup → ForEach → Copy` pulls every file over HTTP |
| **Bronze** | ADLS Gen2 | Raw CSVs landed as-is, one folder per entity |
| **Silver** | Databricks / PySpark | Type-cast, cleansed, enriched, and converted to Parquet |
| **Gold** | Azure Synapse (serverless SQL) | `OPENROWSET` views + one materialized external table, secured via Managed Identity |
| **Reporting** | Power BI | Consumes the Synapse serving layer (per architecture diagram) |

---

## ⚡ Key Features

### 1. **Metadata-Driven, Config-First Ingestion**
Adding an 11th source dataset means adding one entry to `git.json` — not authoring a new pipeline. The repo intentionally keeps the earlier hardcoded `GitToRaw.json` alongside `DynamicGitToRaw.json` to show the refactor.

### 2. **Direct HTTP-to-Lake Ingestion**
No intermediate staging — ADF's `HttpServer` linked service reads CSVs straight from `raw.githubusercontent.com` into ADLS Gen2.

### 3. **Security-Conscious Serving Layer**
The Synapse external data source authenticates via a **Managed Identity**-backed database-scoped credential rather than an embedded storage key.

### 4. **Format-Aware Transformation**
Every Silver write converts CSV to **Parquet with Snappy compression**, and the Gold layer is built entirely on `OPENROWSET`/external tables over that Parquet — no data duplication between Silver and Gold.

### 5. **Multi-File Consolidation**
Three years of sales data (`Sales_2015/2016/2017`), ingested as three separate Bronze files, are consolidated into a single Silver `Sales` dataset via a wildcard read in the same PySpark pass.

---

## 🚀 Setup & Deployment

### Prerequisites
- An Azure subscription with an Azure Data Factory instance, ADLS Gen2 storage account, Azure Databricks workspace, and Azure Synapse Analytics workspace
- Git integration configured on the Data Factory (to publish `adf/pipeline`, `adf/dataset`, `adf/linkedService`)

### Step 1: Deploy the ADF Artifacts
Import `adf/linkedService/*.json`, `adf/dataset/*.json`, and `adf/pipeline/*.json` into your Data Factory (via Git integration or manual import), then update `storage.json` and `httplinkedservice.json` connection details for your own storage account.

### Step 2: Upload the Control File
Place `parameters/git.json` into the `parameters` file system of your ADLS Gen2 account (or point `ds_git_parameters.json` at your own control-file location).

### Step 3: Run the Ingestion Pipeline
Trigger `DynamicGitToRaw` — this lands all 10 source CSVs into the `bronze` file system.

### Step 4: Run the Silver Transformation
Attach `(Clone) silver_layer (without credentials).ipynb` to a Databricks cluster with access to the storage account (via Managed Identity, an OAuth app, or a mounted point) and run all cells to produce Silver Parquet.

### Step 5: Build the Gold Layer
Execute the Synapse SQL scripts in order: `Create Schema.json` → `Create external table.json` → `CREATE VIEWS GOLD.json`.

### Step 6: Connect Power BI
Point a Power BI dataset at the Synapse serverless SQL endpoint and query the `gold.*` views/external tables directly.

---

## 💼 Use Cases

### 1. **Sales by Product Category and Region**
```sql
SELECT
    t.Region,
    p.ProductName,
    SUM(s.OrderQuantity) AS units_sold
FROM gold.sales s
JOIN gold.products p ON s.ProductKey = p.ProductKey
JOIN gold.territories t ON s.TerritoryKey = t.TerritoryKey
GROUP BY t.Region, p.ProductName
ORDER BY units_sold DESC;
```

### 2. **Monthly Order Volume**
```sql
SELECT
    c.Year,
    c.Month,
    COUNT(DISTINCT s.OrderNumber) AS total_orders
FROM gold.sales s
JOIN gold.calendar c ON s.OrderDate = c.Date
GROUP BY c.Year, c.Month
ORDER BY c.Year, c.Month;
```

### 3. **Return Rate by Product**
```sql
SELECT
    p.ProductName,
    COUNT(r.ReturnQuantity) AS total_returns
FROM gold.returns r
JOIN gold.products p ON r.ProductKey = p.ProductKey
GROUP BY p.ProductName
ORDER BY total_returns DESC;
```

---

## 📁 Dataset

AdventureWorks sample retail data, ingested from GitHub as 10 CSV files:

| Dataset | Rows |
|---|---|
| Customers | 18,148 |
| Sales (2015) | 2,630 |
| Sales (2016) | 23,935 |
| Sales (2017) | 29,481 |
| Returns | 1,809 |
| Calendar | 912 |
| Products | 293 |
| Product Subcategories | 37 |
| Territories | 10 |
| Product Categories | 4 |
| **Total** | **~77,000 records** |

---

## 🗺️ Roadmap & Known Limitations

A few things worth flagging for anyone extending this project:

- **Silver write mix-up (Returns)**: the notebook's Returns write cell calls `df_prod.write...option('Path', '.../Returns')` — the Products DataFrame, not `df_returns`, is what actually gets written to the Silver `Returns` folder. The intended `df_returns.write(...)` call is missing.
- **Silver write mix-up (Customers)**: similarly, the final write cell sends `df_sales` to the Silver `Customers` path. The transformed customer DataFrame (`df_cust`, with the derived `fullName` column) is built and displayed earlier in the notebook but is never actually persisted to Silver.
- **Product Categories never reaches Silver**: `df_cat` is read and displayed but has no corresponding `.write()` call — consistent with there being no `gold.categories` view in the Synapse script either.
- **Non-idempotent writes**: every Silver write uses `.mode('append')`. Re-running the notebook without a dedupe/upsert step (or switching to `overwrite`/`MERGE`) will duplicate every row on each run.
- **Hardcoded secret in source control**: `Create external table.json` includes `CREATE MASTER KEY ... ENCRYPTION BY PASSWORD = '...'` with a plaintext password committed to the repo — this should move to Azure Key Vault or a secure pipeline parameter.
- **File extension mismatch**: Bronze sink files are written with the ADF `DelimitedTextWriteSettings` default `fileExtension: ".txt"`, even though the content is comma-delimited CSV — worth aligning for clarity.
- **Power BI isn't in the repo**: the architecture diagram's reporting layer has no committed `.pbix` — presumably built directly against the Synapse serverless endpoint outside version control.
