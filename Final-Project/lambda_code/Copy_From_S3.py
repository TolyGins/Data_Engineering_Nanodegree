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
FROM '{}' 
credentials 'aws_iam_role={}'
format as json {}
STATUPDATE ON
region 'us-west-2'
""")


copy_kochava = ("""
COPY public.kochava_installs
 FROM {}
 credentials 'aws_iam_role={}'
 format as json 'auto'
 gzip
 """
).format (KOCHAVA_PATH, ARN)

copy_singular = (f"""
COPY public.singular_raw
 FROM {SINGULAR_PATH}
 credentials 'aws_iam_role={ARN}'
 csv
 delimiter as ',' 
 gzip
 """
)

copy_comands =[copy_stage]