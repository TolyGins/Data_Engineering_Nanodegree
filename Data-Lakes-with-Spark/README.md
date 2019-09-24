# Project: Data Lake

## Introduction

*A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.*

In this project, we will build an ETL pipeline that extracts data from S3, processes them using Spark, which can be deployed on an EMR cluster, and writes the processed data back into S3 in parquet format. 


## How to run

*To run this project in local mode*, create a file `dl.cfg` in the root of this project with the following data:

```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

Create an S3 Bucket where processed results will be stored.

Finally, run the etl.py script


## Project structure

The files found at this project are the following:

- dl.cfg: config file that contains the AWS credentials
- etl.py: Program that extracts songs and log data from S3, transforms it using Spark, and writes processed data in parquet format back to S3.
- schemas.py : This script contains the table structure that spark uses when reading json files
- README.md: Current file, contains detailed information about the project.

## ETL pipeline

1. Load credentials
2. Read data from S3
    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`

    The script reads song_data and load_data from S3.

3. Process data using spark
    
    Subset the data by relevant columns, perform joins and other transformations.


4. Write the data back to S3

    Writes them to partitioned parquet files.
