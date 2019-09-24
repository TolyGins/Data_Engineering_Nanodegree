from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """ This class runs a redshift copy command from S3 loading our data into stage tables.
    
     Args:
        redshift_conn_id: Redshift connection name that is stored in Airflow server
        aws_credentials : AWS credebtial name that is stored in Airflow server
        table_name : Name of the table to execute the copy command
        s3_bucket: S3 url where the data lives
        file_type : File format e.g. JSON vs CSV
        format_type : URL that points to the format and structure of the raw data
        
        
    Returns:
        Runs the specified SQL
     """
            
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS {} '{}'
        TIMEFORMAT AS 'epochmillisecs'
        region 'us-west-2'
        """
    
    

    @apply_defaults
    def __init__(self,
                redshift_conn_id = 'redshift_cred',
                aws_credentials = '',
                table_name = '',
                s3_bucket = '',
                file_type = 'JSON',
                format_type = '',
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.file_type = file_type
        self.format_type = format_type
        self.execution_date = kwargs.get('execution_date')


    def execute(self, context):
        # Getting credentials
        self.log.info('Getting credentials ')
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Generating the S3 path
        self.log.info("Copying data from S3 to Redshift")
        s3_path = self.s3_bucket
        # Load based on date
        if self.execution_date:
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            s3_path = '/'.join([s3_path, str(year), str(month)])
        
        # Formatting and running sql
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_type,
            self.format_type
            
            
            
        )
        redshift.run(formatted_sql)
        self.log.info(f"Success: Copying {self.table_name} from S3 to Redshift")



