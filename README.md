# Azure Data Engineering Project

End-to-end Azure data engineering project built around the AdventureWorks dataset. The repo shows how to ingest CSV files from GitHub into Azure Data Lake Storage Gen2 with Azure Data Factory, using a scalable metadata-driven pattern.

## Overview

This project:

1. Reads AdventureWorks CSV files from GitHub raw URLs.
2. Copies them into an Azure Data Lake Storage Gen2 account.
3. Organizes the loaded data in a bronze layer for downstream analytics.
4. Uses Azure Data Factory linked services, datasets, and pipelines to orchestrate the flow.

## Architecture

GitHub CSV files -> Azure Data Factory -> ADLS Gen2 bronze layer -> Databricks / Synapse analytics

![End-to-end architecture](architecture.png)

## Repository structure

| Path | Purpose |
| --- | --- |
| `datasets/` | Source CSV files used by the project |
| `dataset/` | ADF dataset JSON definitions |
| `linkedService/` | ADF linked service definitions |
| `pipeline/` | ADF pipeline definitions |
| `factory/` | Azure Data Factory metadata |
| `git.json` | File mapping metadata for the dynamic pipeline |
| `publish_config.json` | ADF publish configuration |

## Data source

The project uses these AdventureWorks files:

- `AdventureWorks_Calendar.csv`
- `AdventureWorks_Customers.csv`
- `AdventureWorks_Product_Categories.csv`
- `AdventureWorks_Product_Subcategories.csv`
- `AdventureWorks_Products.csv`
- `AdventureWorks_Returns.csv`
- `AdventureWorks_Sales_2015.csv`
- `AdventureWorks_Sales_2016.csv`
- `AdventureWorks_Sales_2017.csv`
- `AdventureWorks_Territories.csv`

## Pipelines

### `pipeline1`

A basic copy pipeline that moves `AdventureWorks_Products.csv` from GitHub raw into:

`bronze/products/products.csv`

### `DynamicGitToRaw`

A metadata-driven pipeline that:

1. Reads mappings from `git.json`
2. Loops through each file definition
3. Copies the source CSV from GitHub raw
4. Writes it to the configured bronze folder and file name

## Linked services

- `httplinkedservice` - connects to `https://raw.githubusercontent.com`
- `storage` - connects to the Azure Data Lake Storage Gen2 account

## Datasets

- `ds_http` - fixed GitHub raw source for the sample copy pipeline
- `ds_git_dynamic` - dynamic GitHub raw source using a relative URL parameter
- `ds_git_parameters` - lookup dataset that reads `git.json`
- `ds_sink_dynamic` - dynamic bronze sink with folder and file parameters
- `ds_bronze` - fixed bronze sink for the sample pipeline

## How it works

The dynamic pipeline uses `git.json` as its control file. Each record contains:

- `p_relative_url` - source file path in GitHub
- `p_sink_folder` - target folder in the bronze layer
- `p_sink_file` - target file name

This makes it easy to add new source files without redesigning the pipeline.

## Output location

Loaded files are written to the `bronze` file system in ADLS Gen2, grouped by subject area such as:

- `bronze/Calendar`
- `bronze/Customers`
- `bronze/Products`
- `bronze/Returns`
- `bronze/Sales_2015`
- `bronze/Sales_2016`
- `bronze/Sales_2017`
- `bronze/Territories`

## Notes

- The project is based on the AdventureWorks data engineering walkthrough in the linked video.
- The repo is focused on ingestion and bronze-layer landing, so transformation logic is expected to happen later in Databricks or Synapse.

## Diagram

![Azure pipeline architecture](readme/azure_pipeline_architecture (1).drawio.png)

