## Introduction

This project required me to build an ETL pipeline for user song data. The data, which is in JSON format, is composed of user logs and song data files. I was tasked with setting up a Redshift Cluster, and copying the data from S3 into redshift in it's raw form, prior to loading that data into various tables using the STAR schema.

### Project Structure

* create_tables.py  setups the database connection and runs sql_queries.py
* sql_queries.py contains all the SQL statements for dropping, creating, copying and inserting data into tables
* dwh.cfg config file that contains all necessery parameters that are used in the previous scripts
* etl.py runs the copy and insert sql statements so that data is loaded into our tables
* README.md outlines the structure of the project

### Running the project

1. Create an IAM role with admin access and attach policies for redshift.
2. Create a security group 
3. Use the role and security group and attach them to the redshift cluser
4. Open the terminal and run the create_tables.py script, which will establish a connection to the database and create the tables in sql_queries.py
5. Run etl.py which will filter and transform the data in our files and insert it into our tables

### Database Schema

#### Staging Tables - This is our raw data from user logs and songs

##### Events

* artist:: VARCHAR
* auth:: VARCHAR
* firstName:: VARCHAR
* gender:: VARCHAR
* itemInSession:: VARCHAR
* lastName:: VARCHAR
* length:: VARCHAR DISTKEY
* level:: VARCHAR
* location:: VARCHAR
* method:: VARCHAR
* page:: VARCHAR
* registration:: VARCHAR
* sessionId:: VARCHAR
* song:: VARCHAR
* status:: VARCHAR
* ts:: VARCHAR SORTKEY
* userAgent:: VARCHAR
* userId:: VARCHAR

##### Songs

* artist_id:: VARCHAR
* artist_latitude:: VARCHAR
* artist_location:: VARCHAR
* artist_longitude:: VARCHAR
* artist_name:: VARCHAR
* duration:: VARCHAR  DISTKEY
* num_songs:: VARCHAR
* song_id:: VARCHAR
* title:: VARCHAR
* year:: VARCHAR


#### Fact Table

##### songplays - holds the join of log and song data associated with NextSong action of

* songplay_id:: IDENTITY(0,1) PRIMARY KEY
* start_time:: TIMESTAMP
* user_id:: INT
* level:: VARCHAR
* song_id:: VARCHAR
* artist_id:: VARCHAR 
* session_id:: VARCHAR
* location:: VARCHAR
* user_agent:: VARCHAR


#### Dimension Tables

##### users - holds user information  
* user_id (INT) PRIMARY KEY
* first_name:: VARCHAR
* last_name:: VARCHAR
* gender::VARCHAR
* level:: VARCHAR
* songs:: VARCHAR

##### songs - holds song information 

* song_id (VARCHAR) PRIMARY KEY
* title:: VARCHAR 
* artist_id:: VARCHAR
* year :: INT
* duration:: FLOAT
* artists:: VARCHAR

##### artists - holds dimension related to the artist

* artist_id:: (VARCHAR) PRIMARY KEY
* name:: VARCHAR
* location::VARCHAR 
* lattitude:: FLOAT
* longitude:: FLOAT
* time:: TIMESTAMP

##### time- holds diffferent level of aggregation for the time attribute

* start_time:: (TIMESTAMP) PRIMARY KEY
* hour:: INT
* day:: INT
* week:: INT
* month:: INT
* year:: INT
* weekday:: INT
