from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info('DataQualityOperator started')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for test in self.tests:
            for table in self.tables:
                records = redshift_hook.get_records(test.format(table))
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError("Data quality check failed. {table} returned no results".format(table=table))
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError("Data quality check failed. {table} contained 0 rows".format(table=table))
                self.log.info("Data quality on table {table} check passed with {result} records".format(table=table, result=records[0][0]))
