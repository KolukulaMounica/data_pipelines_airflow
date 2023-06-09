from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 skip=False,
                 *args, **kwargs):        

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.skip = skip
        
        
    def execute(self, context):
        if self.skip == False:
            self.log.info("Creating Redshift Tables")        
            redshift = PostgresHook(self.redshift_conn_id)
            redshift.run(self.sql)
        else:
            self.log.info("Skip creating Redshift schema")  
        