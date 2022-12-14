from airflow.models import DAG 
from airflow.decorators import task

from datetime import datetime

default_args = {
	'start_date': datetime(2021, 1, 1)
}

with DAG('process_dag',
	schedule_interval='@daily',
	default_args=default_args,
	catchup=False
	) as dag:

	@task.python
	def t1():
		print("t1")

	# @task.python
	# def t2():
	# 	print("t2")

	@task.python
	def t3():
		print("t3")

	t1() >> t3()