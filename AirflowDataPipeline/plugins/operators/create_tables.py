from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):

    ui_color = '#F90066'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names="",
                 queries_to_run="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries_to_run = queries_to_run
        self.table_names = table_names

    def execute(self, context):
        self.log.info('CreateTablesOperator started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.table_names:
            self.log.info("Deleted table {}".format(table))
            redshift.run("DROP TABLE IF EXISTS {}".format(table))

        # execute multiple queries
        if isinstance(self.queries_to_run, list):
            for query in self.queries_to_run:
                self.log.info("Running query {}".format(query))
                redshift.run(query)
        # or one query
        elif isinstance(self.queries_to_run, str):
                redshift.run(self.queries_to_run)
        else:
            raise ValueError("Queries to run should be a string or a list of strings")
        self.log.info('CreateTablesOperator finished')
