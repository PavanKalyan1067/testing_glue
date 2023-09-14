---

# Semantic Layer Creation

## Overview

This Python script is designed to create dimension and fact tables from various data sources. It leverages AWS Glue and Apache Spark for data transformation and processing.

## Getting Started

Before running the script, ensure you have the necessary modules and libraries installed. You can set up your environment by following these steps:

### Importing Relevant Modules and Starting Spark Session

The script begins by importing essential modules and setting up the Spark and Glue contexts:

- `sys`, `getResolvedOptions`, `SparkContext`, `GlueContext`, `Job`: These modules provide access to system parameters and AWS Glue-specific functionality.

- `pyspark`, `SparkSession`, `functions`, `col`, `unix_timestamp`, `from_unixtime`, `date_format`, `Window`: These modules are crucial for Apache Spark operations and data transformations, especially for timestamp and date-time handling.

- `sc`, `glueContext`, `spark`, `job`: These variables initialize the Spark and Glue contexts and define an AWS Glue job for distributed data processing.

## Reading Data from Sources

The script reads data from multiple sources stored in S3 buckets. It performs pre-processing on each DataFrame to prepare the data for further analysis:

- Alarm Data: Reads 'alarm' data.

- Event Data: Reads 'events' data.

- Location Data: Reads 'locations' data.

- Transformed Location Data: Reads 'transformed_location_details' data.

- SFDC Data: Reads 'sfdc' data.

- Facilities Data: Reads 'facilities' data.

## Data Pre-processing

The script preprocesses each DataFrame by excluding milliseconds from timestamp columns. It performs these operations for each DataFrame separately.

## Semantic Tables Creation

The script creates dimension and fact tables by joining and selecting fields from the processed data:

- Location Dimension Table: Derived by joining 'loc_df' with 'trans_loc_df'.

- Resident Dimension Table: Selected fields from 'alarms_df'.

- Events Dimension Table: Selected fields from 'events_df'.

- Community Dimension Table: Derived by joining 'events_df' with 'sfdc_df'.

- Alarms Fact Table: Selected fields from 'alarms_df' with additional calculated fields.

## Data Filtering

The script applies filtering rules to the 'alarms' table, excluding specific records based on the following conditions:
- 1.Filter out records with acceptance time less than 5 seconds (0.0833 minutes)
- 2.Filter out records where 'openedat' and 'closedat' is null
- 3.Filter out records that don't have "QA" or "Dev" in the facility and community columns
- 4.Create a separate table for alarms that have acceptance time 2+ hours.
## Writing to S3 Buckets

Finally, the script converts DataFrames into Dynamic Frames and writes them to corresponding S3 buckets as Parquet files.

- Resident Data: Written to 's3://test.processing.for.glue.prod.output1/resident/'.

- Events Data: Written to 's3://test.processing.for.glue.prod.output1/events/'.

- Community Data: Written to 's3://test.processing.for.glue.prod.output1/community/'.

- Location Data: Written to 's3://test.processing.for.glue.prod.output1/location/'.

- Alarms Data: Written to 's3://test.processing.for.glue.prod.output1/alarms/'.

- Alarms Outliers: Records with acceptance time greater than or equal to 2 hours are written to 's3://test.processing.for.glue.prod.output1/alarms_graterthan_2hrs/'.

---

This README provides an overview of the Python script's purpose, key functionalities, and the steps involved in data processing and transformation. To run the script successfully, make sure you have the required AWS Glue and Spark configurations and access to the specified S3 buckets.
