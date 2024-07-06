from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.create_table_exec import create_tables_func
from helpers.drop_tables_exec import drop_tables_func

class CreateTableOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        *args, 
        **kwargs
    ):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        drop_tables_func(redshift)
        create_tables_func(redshift)

        self.log.info("Created table in Redshift")