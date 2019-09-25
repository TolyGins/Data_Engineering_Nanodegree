import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
EVENT_DATA_PATH  = config.get('S3', 'EVENT_DATA_PATH')
EVENT_SCHEMA    = config.get('S3', 'EVENT_SCHEMA')
KOCHAVA_PATH    = config.get('S3', 'KOCHAVA_PATH')
SINGULAR_PATH = config.get('S3', 'SINGULAR_PATH')

copy_stage = ("""
COPY victory.raw_events
FROM {} 
credentials 'aws_iam_role={}'
format as json {}
STATUPDATE ON
region 'us-west-2'
""").format (EVENT_DATA_PATH, ARN, EVENT_SCHEMA)


copy_kochava = ("""
COPY public.kochava_installs
 FROM {}
 credentials 'aws_iam_role={}'
 csv
 delimiter as ',' 
 gzip
 """
).format (KOCHAVA_PATH, ARN)

copy_singular = ("""
COPY public.singular_raw
 FROM {}
 credentials 'aws_iam_role={}'
 csv
 delimiter as ',' 
 gzip
 """
).format (SINGULAR_PATH, ARN)

copy_comands =[copy_singular, copy_kochava, copy_stage]