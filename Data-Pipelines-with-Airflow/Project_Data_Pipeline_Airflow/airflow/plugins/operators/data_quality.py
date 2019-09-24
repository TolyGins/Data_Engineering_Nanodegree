from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ This class runs data quality checks on a list of tables, countring the number of rows in each table.
    
     Args:
        redshift_conn_id: Redshift connection name that is stored in Airflow server
        table_names : List of table names to run the checks on 
        database: The database name
              
    Returns:
        Runs the formatted SQL
     """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = 'redshift_conn',
                table_names = 'list_of_table_names',
                database = 'your_db',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names
        self.database = database 

    def execute(self, context):
        self.log.info('Getting Credentialds')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info('Running data quality checks')
        for table in self.table_names:
            print ("Table check is running")
            print ("----------------------")
            print (table)
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                break
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                break 
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")