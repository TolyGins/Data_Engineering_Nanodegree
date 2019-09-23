## Introduction

This project required me to build an ETL pipeline for user song data. The data, which is in JSON format, is composed of user logs and song data files. I was tasked with not only developing the data pipeline that ingests and transforms this data, but also develop and database schema that organizes the data into normalized tables. 

### Project Structure

* data folder is the home folder where all data resides 
* sql_queries.py contains all the SQL statements for dropping, creating and inserting data into tables
* create_tables.py setups the database connection and runs sql_queries.py
* etl.ipynb is a python notebook that transforms and inserts data one row at a time
* etl.py is a production script that bulk transforms and insert the data into our tables
* test.ipynb is a notebook that allows you to query the tables after data has been loaded
* README.md outlines the structure of the project

### Running the project

1. Open the terminal and run the create_tables.py script, which will establish a connection to the database and create the tables in sql_queries.py
2. Run etl.py which will filter and transform the data in our files and bulk insert it into our tables
3. Run all the cells in test.ipynb to preview the data in each of our tables

### Database Schema

#### Fact Table

##### songplays - holds the join of log and song data associated with NextSong action of

* songplay_id:: (SERIAL) PRIMARY KEY
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
