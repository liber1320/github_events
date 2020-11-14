from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension table in Redshift from data in staging table
    :param redshift_conn_id: Redshift connection ID
    :param table: Target table in Redshift to load
    :param select_sql: SQL query for getting data to load into target table
    :param key: The column to check if the row already exists in the target table. If there is a match, the
        row in the target table will then be preserved without change
    """
    
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 key1="",
                 key2="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.key1 = key1
        self.key2 = key2

    def execute(self, context):
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        table_insert_sql = f"""
                insert into {self.table}_staging
                {self.select_sql};

                delete from {self.table}_staging
                using {self.table}
                where {self.table}.{self.key1} = {self.table}_staging.{self.key1}
                and {self.table}.{self.key2} = {self.table}_staging.{self.key2};

                insert into {self.table} ({self.key1}, {self.key2})
                select * from {self.table}_staging;
             """

        self.log.info("Loading data into dimension table in Redshift")
        redshift_hook.run(table_insert_sql)