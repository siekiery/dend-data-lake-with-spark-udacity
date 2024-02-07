# Project: Data Lake
###  Data Lakes with Spark / Data Engineer Nanodegree
### Udacity
### Author: Jakub Pitera
___

This projects concerns building a cloud data lake ETL pipeline. It extracts data from input S3, transforms it using pyspark and loads into output S3. It applies AWS, ETL and data lake knowledge learned in a course. 

### Pipeline steps: 
1. Starts spark session
2. Load song data from input S3 bucket
3. Transform and load songs and artists tables as partitioned parquet files onto output S3
4. Load log data from input S3 bucket
5. Transform and load users, time and songplays tables as partitioned parquet files onto output S3

In order to run the pipeline run all cells in etl.ipynb notebook

### Files description:
* etl.ipynb - notebook file to run the pipeline
* create_tables.py - python script for creating and dropping tables
* dl.cfg - config file with AWS user credentials
* etl.py - python script for ETL pipeline instructions
* README.md - documentation