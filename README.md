# Azure Data Engineering Project

This repository implements an AdventureWorks ingestion pipeline on Azure using **Azure Data Factory (ADF)** for orchestration and **Azure Synapse SQL scripts** for serving-layer setup.

## Project overview

This project ingests AdventureWorks CSV data from GitHub into ADLS Gen2 and organizes it for analytics:

1. **Source**: AdventureWorks CSV files hosted in GitHub.
2. **Ingestion**: ADF pipelines copy files from GitHub raw URLs.
3. **Storage**: Files land in the ADLS **bronze** layer.
4. **Serving prep**: Synapse SQL scripts create schema, external objects, and views over curated data.

## Data architecture

The implemented architecture follows a layered lakehouse-style flow:

`GitHub CSV (source) -> ADF ingestion -> ADLS bronze -> (transform to silver outside this repo flow) -> Synapse gold views`

![Azure pipeline architecture](holding/azure_pipeline_architecture%20(1).drawio.png)

## Current repository structure

```text
aw-data-pipeline/
├── adf/
│   ├── pipeline/
│   ├── dataset/
│   └── linkedService/
├── synapse/
│   └── sqlscipts/
├── databricks/
│   └── notebooks/
├── parameters/
│   └── git.json
├── holding/
└── README.md
```

## What is implemented

| Area | Asset | Purpose |
| --- | --- | --- |
| ADF Pipeline | `adf/pipeline/GitToRaw.json` | Single-file copy from GitHub raw to ADLS bronze (`products/products.csv`) |
| ADF Pipeline | `adf/pipeline/DynamicGitToRaw.json` | Metadata-driven multi-file ingestion using Lookup + ForEach + Copy |
| ADF Dataset | `adf/dataset/ds_git_parameters.json` | Reads `git.json` from ADLS file system `parameters` |
| ADF Dataset | `adf/dataset/ds_git_dynamic.json` | Parameterized HTTP source (`p_relative_url`) |
| ADF Dataset | `adf/dataset/ds_sink_dynamic.json` | Parameterized ADLS bronze sink (`p_sink_folder`, `p_filename`) |
| ADF Linked Service | `adf/linkedService/httplinkedservice.json` | GitHub raw endpoint (`https://raw.githubusercontent.com`) |
| ADF Linked Service | `adf/linkedService/storage.json` | ADLS Gen2 account connection |
| Synapse SQL | `synapse/sqlscipts/*.json` | Schema creation, external objects, and gold views |

## Data ingestion design

`DynamicGitToRaw` works in this sequence:

1. `LookupGit` reads the control records from `git.json`.
2. `ForEachGit` iterates through each record (currently sequential).
3. `DynamicCopy` reads CSV from GitHub raw using `p_relative_url`.
4. Output lands in ADLS **bronze** file system using `p_sink_folder` and `p_sink_file`.

The control file format in `parameters/git.json`:

- `p_relative_url`: path under `raw.githubusercontent.com`
- `p_sink_folder`: target bronze folder
- `p_sink_file`: target file name

## Source data covered

The parameter file currently maps these AdventureWorks datasets:

- Calendar
- Customers
- Product Categories
- Product Subcategories
- Products
- Returns
- Sales (2015, 2016, 2017)
- Territories

## Synapse layer

The Synapse SQL scripts currently cover:

1. Creating schema `gold`
2. Creating external data source(s), file format, and external table
3. Creating gold views over Parquet in the `silver` container
4. Query example (`select * from gold.customer`)

## Important repository note after cleanup

Files that were not part of the active structure were moved to `holding/` (not deleted), including:

- Source CSV files (`holding/datasets/`)
- Factory/integration runtime/credential exports
- Architecture diagram image and publish config

If you use this repository itself as the GitHub raw source, make sure `parameters/git.json` paths match the real location of the CSV files (for example, update `p_relative_url` if files stay under `holding/datasets/`).
