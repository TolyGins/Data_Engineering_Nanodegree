from airflow.hooks.postgres_hook import PostgresHook
#from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class RunSQLFileOperator(BaseOperator):
    """ This class runs an unformatted SQL command on our redshift cluster.
    
     Args:
        redshift_conn_id: Redshift connection name that is stored in Airflow server
        sql_file : A SQL file that you'd like to run
        database: The database name
              
    Returns:
        Runs the formatted SQL
     """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                sql_file = 'some_sql_file',
                database = 'your_db',
                 *args, **kwargs):
        super(RunSQLFileOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_file = sql_file
        self.database = database

    def execute(self, context):
        self.log.info('Getting Credentialds')
        redshift = PostgresHook(self.redshift_conn_id)


        self.log.info('Running SQL File')
        redshift.run(self.sql_file)
        
     

# test = RunSQLFileOperator(task_id = 'Create_tables',
#     redshift_conn_id ='redshift',
#     aws_credentials ='aws_credentials',                      
#     sql_file = 'some-statement',
#     database = 'dev' )
# test.execute('some context')