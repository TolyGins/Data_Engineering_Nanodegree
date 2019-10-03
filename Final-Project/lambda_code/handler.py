import configparser
import psycopg2
from Copy_From_S3 import copy_comands, EVENT_DATA_PATH,ARN, EVENT_SCHEMA


def sql_exec(cur, conn, query_list, full_address=None):
    """This function executes a list of SQL commands found in sql_queries module
    
    Args:
        cur: This is the db cursor from psychopg2
        conn: This is the db connection from psychopg2
        query_list : This is the list of queries we are passing
        full_address : This is the full address to the file that needs to be copied

    Returns:
        The return in this function is actually commiting the sql execution to the db """

    for query in query_list:
        print (query.format(full_address, ARN, EVENT_SCHEMA))
        cur.execute(query.format(full_address, ARN, EVENT_SCHEMA))
        conn.commit()

lambda_code = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-west-2', 'eventTime': '2019-09-27T19:10:26.869Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'AWS:AIDAJEXLEFNZVL74YXFNU'}, 'requestParameters': {'sourceIPAddress': '63.237.117.50'}, 'responseElements': {'x-amz-request-id': '751F84B87085E3FA', 'x-amz-id-2': '5YI3GGSCle/wICRSQnQRAbqCkVTXCmitHu5NcLOA0fFx29jT8CnFID5XH0yeeU/qndejPlnljyE='}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'c19d21d5-cd41-4a74-bd9a-f0090889c785', 'bucket': {'name': 'tg-test-bucket-data-engineering', 'ownerIdentity': {'principalId': 'ASRS9YTFYIWM6'}, 'arn': 'arn:aws:s3:::tg-test-bucket-data-engineering'}, 'object': {'key': 'staged/2019/09/27/20/yeti-victory-test-1-2019-09-19-20-00-20-d569186b-a21a-434a-b892-757c6f5a84a5d2a35914-a39c-4554-9d42-3f1f7ac12f38', 'size': 5697349, 'eTag': '8d21ed23e6cc6f9470bec17a7a6db90a', 'sequencer': '005D8E5EA2A3487FE7'}}}]}

def lambda_handler(event, context):
    print (f"lambda event : {event} ")

    # Get a File to Load into the Copy Command

    file_name = event['Records'][0]['s3']['object']['key']

    full_address = str(EVENT_DATA_PATH+file_name) 

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    sql_exec(cur, conn, copy_comands,full_address)

