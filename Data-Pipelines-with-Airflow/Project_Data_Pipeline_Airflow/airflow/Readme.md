
## Project Description

The goal of this project was to load raw data from S3 into Redshift tables using a defined DAG with Apache Airflow. The DAG allowed us to confirm that task dependencies are met prior to running FACT and Dimensional tables that will be used in production. The structure of the DAG is as follows:

1. Create staging, fact and dimensional tables in Redshift 
2. Load raw data from S3 to Redshift into Staging Tables
3. Load data into the SongPlays Fact table
4. Load data into all the dimension tables

### Project Files

```
airflow
    dags
        *udac_example_dag.py : This is where we implement our operators and structure the logic of our DAG
    plugins
        helpers
            * sql_queries.py: This file contains our SQL syntax for creating and inserting data into tables
        operators
            * create_tables.py: This operator runs a SQL file in our Redshift cluster 
            * stage_redshift.py: This operator copies data from S3 to Redshift
            * load_dimension.py: This operator loads data into FACT or Dimension tables
            * data_quality.py : This operator conducts data quality checks on all our tables
```

### Datasets

** Song Data Path ** --> s3://udacity-dend/song_data 

** Log Data Path **  --> s3://udacity-dend/log_data

### DAG Structure

![DAG_Graph_view](https://user-images.githubusercontent.com/10493680/65535284-56a02880-debe-11e9-963d-d30cb32eff8a.png)


### DAG Configuration

This DAG has the following parameters:

* The DAG does not have dependencies on past runs
* On failure, the task are retried 3 times
* Retries occur every 5 minutes
* Catchup is turned off
* Do not email on retry

### Operators

In order to implement the DAG tasks we have created operators that do the following:
* Run SQL files in Redshift
* Load data from S3 to Redshift based on a specified start date
* Create DIM or FACT tables based on entered parameters
* Run data quality checks on any desired tables

### Airflow Instructions

When you are in the workspace, you can start by using the command : /opt/airflow/start.sh. This will launch the Airflow host and automatically start all the dags required and outputting the result to its respective tables. 
