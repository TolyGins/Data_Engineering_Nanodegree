import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
EVENT_DATA_PATH  = config.get('S3', 'EVENT_DATA_PATH')
KOCHAVA_DATA   = config.get('S3', 'KOCHAVA_PATH')

# SCHEMA
victory_schema = "CREATE SCHEMA IF NOT EXISTS victory"


# DROP TABLES STATEMENTS

staging_events_table_drop = "DROP TABLE IF EXISTS victory.raw_events"
sessions_drop = "DROP TABLE IF EXISTS victory.sessions"
kochava_drop = "DROP TABLE IF EXISTS public.kochava_installs"
singular_drop = "DROP TABLE IF EXISTS public.singular_raw"

# CREATE TABLES
staging_table = """
CREATE TABLE IF NOT EXISTS victory.raw_events (
    user_id VARCHAR (255)
    , app_ver VARCHAR 
    , platform VARCHAR
    , country VARCHAR
    , event_timestamp BIGINT 
    , session_id VARCHAR
    , event_id VARCHAR
    , session_purchases INT
    , session_value INT
    , event_details VARCHAR (max)
);
"""

sessions_table = """
CREATE TABLE IF NOT EXISTS victory.sessions (
    user_id VARCHAR (255)
   , session_id VARCHAR 
   ,  app_ver VARCHAR 
   , session_start TIMESTAMP 
   , session_end TIMESTAMP
   , country VARCHAR
   , session_purchases INT
   , session_value INT
   );
"""

kochava_table = """
CREATE TABLE IF NOT EXISTS public.kochava_installs (
	    install_id INT
	    , udid VARCHAR
	    , udid_md5 VARCHAR
	    , odin VARCHAR
	    , bid_price FLOAT
	    , matched_on VARCHAR
	    , android_id VARCHAR
	    , cpi_price FLOAT
	    , imei_sha1 VARCHAR
	    , campaign_id VARCHAR
	    , device_ver VARCHAR
	    , idfv VARCHAR
	    , facebook_id VARCHAR
	    , imei VARCHAR
	    , request VARCHAR
	    , click_id VARCHAR(512)
	    , udid_sha1 VARCHAR
	    , site_id VARCHAR(512)
	    , imei_md5 VARCHAR
	    , tracker_name VARCHAR
	    , install_dtm VARCHAR
	    , install_date DATE
	    , click_date DATE
	    , mac VARCHAR
	    , campaign_name VARCHAR
	    , network_id VARCHAR
	    , user_id VARCHAR
	    , country_code VARCHAR
	    , creative VARCHAR
	    , bid_id VARCHAR
	    , android_sha1 VARCHAR
	    , campaign_real_name VARCHAR
	    , android_md5 VARCHAR
	    , matched_by VARCHAR
	    , kochava_device_id VARCHAR
	    , idfa VARCHAR
	    , network_name VARCHAR
	    , idfv_sha1 VARCHAR
	    , tracker_id VARCHAR
	    , adid VARCHAR
	    , app_id VARCHAR
	    , app_name VARCHAR
	    , postback VARCHAR
	    , api_key VARCHAR
	    , row_id INT
	    , insert_time timestamp
	    , attribution_creative VARCHAR
	    , cp_name1 VARCHAR
	    , cp_value1 VARCHAR
	    , install_price DOUBLE PRECISION
	    , fire_adid VARCHAR
		, cp_3 VARCHAR
	    , "iad.iad-conversion-type" VARCHAR
		, cp_2 VARCHAR
		, googl_v2_campaign_name VARCHAR
        , load_time timestamp
	);
"""

singular_table = """
CREATE TABLE IF NOT EXISTS public.singular_raw (
			 activity_date DATE
			 , app VARCHAR
			 , os VARCHAR
			 , source VARCHAR
			 , campaign VARCHAR
			 , country VARCHAR
			 , custom_installs FLOAT
			 , adn_cost FLOAT
			 , adn_clicks FLOAT
			 , adn_impressions FLOAT
			 , adn_installs FLOAT
			 , tracker_installs FLOAT
			 , tracker_clicks FLOAT
			 , alt_source VARCHAR
			 , tracker_name VARCHAR
			 , sub_campaign_name VARCHAR
			 , sub_campaign_id VARCHAR
			 , sub_adnetwork_name VARCHAR
			 , campaign_id VARCHAR
			 , keyword VARCHAR
             , load_time timestamp

	);
"""

# INSERT STATEMENTS
session_insert = """
INSERT INTO victory.sessions (user_id, session_id, app_ver, session_start, session_end, country, session_purchases,session_value)
SELECT  
user_id
, session_id
, app_ver
, MIN (TIMESTAMP 'epoch' + (event_timestamp /1000) * INTERVAL '1 second') AS session_start 
, MAX (TIMESTAMP 'epoch' + (event_timestamp /1000) * INTERVAL '1 second') AS session_end
, country
, MAX(session_purchases) as session_purchases
, MAX(session_value) AS session_value
FROM victory.raw_events
GROUP BY 
user_id
, session_id
, app_ver
, country
;
"""


drop_table_queries = [staging_events_table_drop, sessions_drop, kochava_drop, singular_drop] 
create_table_queries = [staging_table, sessions_table, kochava_table, singular_table]
schemas_create = [victory_schema]
insert_statements= [session_insert]