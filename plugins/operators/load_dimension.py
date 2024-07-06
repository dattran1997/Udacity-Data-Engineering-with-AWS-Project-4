from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql_query="",
        append_mode=True,
        *args, 
        **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_mode = append_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_mode:
            self.log.info("Delete data from {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
            
        self.log.info("Insert data from staging table to {} table".format(self.table))
        redshift.run(self.sql_query)
