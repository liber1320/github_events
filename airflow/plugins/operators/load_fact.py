from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads fact table in Redshift from data in staging table
    :param redshift_conn_id: Redshift connection ID
    :param table: Target table in Redshift to load
    :param insert_sql: SQL query for loading into target table
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql

    def execute(self, context):
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading data into fact table in Redshift")
        table_insert_sql = f"""{self.insert_sql}"""
        redshift_hook.run(table_insert_sql)