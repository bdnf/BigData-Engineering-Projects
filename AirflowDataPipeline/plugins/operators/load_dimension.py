from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = "",
                 sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("INSERT INTO {table} {query}".format(table=self.table, query=self.sql_query))
