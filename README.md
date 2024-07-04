# NYC Yellow Taxi Trip Data: ETL with Apache Spark

This repository demonstrates how to process the NYC TLC Yellow Taxi Trip Records dataset using cloud-based services (Apache Spark within Azure Databricks). The project focuses on leveraging Spark's capabilities to efficiently handle and clean large-scale datasets. The cleaned data is then stored back into Azure Blob Storage.

## Overview

This project processes the NYC Yellow Taxi Trip Records dataset (year 2022) using the following steps:

1. **Data Acquisition**: Load the dataset from the external site (webpage) to the Azure Blob Storage.
2. **Data Cleaning**: Filter and clean the data to ensure its usable for further exploration and analysis.
3. **Data Transformation**: Calculate additional metrics like trip duration.
4. **Data Storage**: Save the cleaned data back to Azure Blob Storage.

By utilizing Apache Spark within Azure Databricks, this project can handle the dataset at scale (~40M records for the year 2022), demonstrating the integration of Spark with Azure services and the use of Object-Oriented Programming (OOP) concepts in PySpark.

## Project Structure

- **`ParqueDownloader` Class**: Contains methods that fetches all the Yellow Taxi Trip Records parquet files from the URL, function that downloads all the parquet files and stores the raw dataset to Azure storage.
- **`DataCleaner` Class**: Contains methods for data loading from Azure blob storage, cleaning, and saving using PySpark.
- **Azure Blob Storage**: Stores both raw and cleaned datasets.
- **Azure Databricks**: Executes the Spark code to process the data efficiently, avoiding performance glitches.

## Prerequisites

To run this project, you need the following:

- An Azure Storage Account and Blob container. Ensure you have your Azure Storage Account details, Container name, SAS token configured properly.
- An Azure Databricks Workspace and a cluster to run the Spark code.
- Apache Spark configured within Azure Databricks.
- Python with PySpark library installed.

## Key points to note before running the code 
- First, run `NYC_taxi_trip_data_load.ipynb` on your Azure Databricks workspace. This notebook instance is responsible for data acquisition and uploading the raw dataset to cloud storage.
The code only retrieves records related to "Yellow Taxi Trip Records" for the year 2022. You can update the "year_to_upload = 2022" code in case you want to work on multiple years of data.
- Secondly, run `NYC_taxi_trip_data_clean_and_storage.ipynb` on your Azure Databricks workspace. This notebook instance handles data cleaning and transformations.
- Thirdly, the notebook mentioned above also manages the storage of the cleaned data in Azure blob storage. Even after cleaning the data (~37 million records), it's still a huge dataset.
- The cleaned data is stored as parquet files, partitioned automatically (~`28.23 MB` each) to handle scalability. All the parquet files (after cleaning) accounts for ~`10 GB` just for the `2022` dataset. Similarly, the NYC TLC dataset contains data ranging from 2009 to 2024.
- Ensure you have a storage account large enough to handles 100s of GB at the minimum.

## How to Run
- Azure Blob Storage Setup: Make sure your storage account and SAS token are correctly configured.
- Deploy on Azure Databricks:
  - Create a new cluster in Azure Databricks.
  - Upload the script to a Databricks notebook.
  - Execute the notebook to run the data processing pipeline.
- Verify the data upload by checking your Azure Storage >> Containers >> container_name >> nycyellowtaxidata-cleaned/2022 (after cleaning & transformations)

## Notes
- Replace `your_sas_token_here` with a valid `SAS token` to access your Azure Blob Storage.
- Modify container_name, storage_account_name, and folder_path according to your specific Azure setup.
