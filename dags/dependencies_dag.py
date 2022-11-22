from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.models.baseoperator import cross_downstream, chain
from datetime import datetime
from airflow.operators.dummy import DummyOperator

default_args = {
	'start_date': datetime(2021, 1, 1)
}

# @dag("dependencies_dag",
# 	schedule_interval='@daily',
# 	default_args=default_args,
# 	catchup=False,
# 	max_active_runs=1
# 	)

# def dependencies_dag():

with DAG('dependencies', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

	t1 = DummyOperator(task_id='t1')
	t2 = DummyOperator(task_id='t2')
	t3 = DummyOperator(task_id='t3')
	t4 = DummyOperator(task_id='t4')
	t5 = DummyOperator(task_id='t5')
	t6 = DummyOperator(task_id='t6')

	cross_downstream([t2,t3], [t4,t5])
	chain(t1,t2,t5,t6)