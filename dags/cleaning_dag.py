import airflow.utils.dates
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor 

default_args = {
	"owner": "bigTuma",
	"start_date": datetime(2021, 1, 1)
}

with DAG(dag_id='cleaning_dag',
	default_args=default_args,
	schedule_interval='@daily',
	catchup=False) as dag:

	waiting_for_task = ExternalTaskSensor(
		task_id='waiting_for_task',
		external_dag_id='my_dag',
		external_task_id='storing')

	cleaning_xcoms = PostgresOperator(
		task_id='cleaning_xcoms',
		sql='sql/cleaning_xcoms.sql',
		postgres_conn_id='postgres'
		)

	waiting_for_task >> cleaning_xcoms