from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTablesOperator(BaseOperator):
    """ This class runs a formatted SQL command for FACT and Dimension tables.
    
     Args:
        redshift_conn_id: Redshift connection name that is stored in Airflow server
        table_name : Name of the table to execute the copy command
        database: Specified database name
        sql_statement : Insert or truncate/insert sql statement
        table_type : DIM or FACT table 
        
    Returns:
        Runs the specified SQL
     """
    
    
    ui_color = '#80BD9E'
    

    dim_insert_template = """ 
    TRUNCATE TABLE {};
    INSERT INTO {}  
        {}
        ; 
    """
    fact_insert_template ="""
    INSERT INTO {} (
        {}
        );
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id = 'redshift_conn',
                table_name = 'some_table',
                database = 'your_db',
                sql_statement = 'your_sql_statement',
                table_type = '',

                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.database = database 
        self.sql_statement = sql_statement
        self.table_type = table_type

    def execute(self, context):
        self.log.info('Getting Credentials')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Deleting tables')
        if self.table_type =='dim':
            self.log.info('Inserting into DIM tables')
            self.log.info('Deleting and Inserting DIM tables')
            formated_sql = LoadTablesOperator.dim_insert_template.format(self.table_name, self.table_name, self.sql_statement)
        else:
            self.log.info('Inserting into FACT tables')
            formated_sql = LoadTablesOperator.fact_insert_template.format(self.table_name,self.sql_statement)
        
        redshift.run(formated_sql)

    
# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults


# class LoadTablesOperator(BaseOperator):

#     ui_color = '#80BD9E'

#     @apply_defaults
#     def __init__(self,
#                  redshift_conn_id="",
#                  sql_query="",
#                  table="",
#                  truncate="",
#                  *args, **kwargs):
#         super(LoadTablesOperator, self).__init__(*args, **kwargs)
#         self.redshift_conn_id = redshift_conn_id
#         self.sql_query = sql_query
#         self.table = table
#         self.truncate = truncate

#     def execute(self, context):
#         """
#           Insert data into dimensional tables from staging events and song data.
#           Using a truncate-insert method to empty target tables prior to load.
#         """
#         redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
#         if self.truncate:
#             redshift.run(f"TRUNCATE TABLE {self.table}")
#         formatted_sql = self.sql_query.format(self.table)
#         redshift.run(formatted_sql)
#         self.log.info(f"Success: {self.task_id}")
