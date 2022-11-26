from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from groups.process_tasks import process_tasks
from airflow.operators.dummy import DummyOperator
import time
from airflow.sensors.date_time import DateTimeSensor
from airflow.exceptions import AirflowTaskTimeout, AirflowSensorTimeout
from aifrlow.operators.trigger_dagrun import TriggerDagRunOperator

# class CustomPostgresOperator(PostgresOperator):

# 	template_fields = ('sql', 'parameters')

partners = {
	"partner_snowflake":
	{
		"name": "snowflake",
		"path": "/partners/snowflake",
		"priority": 2
	},
	"partner_netflix":
	{
		"name": "netflix",
		"path": "/partners/netflix",
		"priority": 3
	},
	"partner_astronomer":
	{
		"name": "astronomer",
		"path": "/partners/astronomer",
		"priority": 1
	},
}

@task.python(task_id='extract_partners', do_xcom_push=False, pool='partner_pool', multiple_outputs=True)
def extract(partner_name, partner_path):
	time.sleep(3)
	return {"partner_name": partner_name, "partner_path": partner_path}
	# partner_settings = Variable.get("my_dag_partner", deserialize_json = True)
	# name = partner_settings['name']
	# api_key = partner_settings['api_secret']
	# path = partner_settings['path']


default_args = {
	"start_date": datetime(2021, 1, 1),
	"retries": 0
}

# def _choosing_partner_based_on_day(execution_date):
# 	day = execution_date.day_of_week
# 	if (day == 1):
# 		return 'extract_partner_snowflake'
# 	if (day == 3):
# 		return 'extract_partner_netflix'
# 	if (day == 5):
# 		return 'extract_partner_astronomer'
# 	return 'stop'


def _success_callback(context):
	print(context)

def _failure_callback(context):
	print(context)

def _extract_callback_success(context):
	print('SUCCESS CALLBACK')

def _extract_callback_failure(context):
	# if (context['exception']):
	# 	if (isinstance(context['exception']), AifFlowTaskTimeout):
	# 	if (isinstance(context['exception']), AifFlowSensorTimeout):
	print('FAILURE CALLBACK')

def _extract_callback_retry(context):
	if (context['ti'].try_number() > 2):
		print('RETRY CALLBACK')

def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
	print(task_list)
	print(blocking_tis)
	print(slas)

@dag("my_dag", 
	description = "Practice DAG",
	default_args=default_args,
	schedule_interval = "@daily",
	dagrun_timeout = timedelta(minutes = 10),
	tags = ["data_eng"],
	catchup = False,
	max_active_runs = 1,
	concurrency=2,
	on_success_callback=_success_callback,
	on_failure_callback=_failure_callback,
	sla_miss_callback=_sla_miss_callback
	)

def my_dag():

	start = DummyOperator(task_id="start", trigger_rule='all_success')

	# delay = DateTimeSensor(
	# 	task_id='delay',
	# 	target_time="{{ execution_date.add(hours9=9) }}",
	# 	poke_interval=60 * 60,
	# 	mode='poke',
	# 	timeout=60 * 60 * 10,
	# 	# execution_timeout=,
	# 	soft_fail=True,
	# 	exponential_backoff=True
	# 	)

	# choosing_partner_based_on_day = BranchPythonOperator(
	# 	task_id='choosing_partner_based_on_day',
	# 	python_callable=_choosing_partner_based_on_day)

	# stop = DummyOperator(task_id='stop')

	storing = DummyOperator(task_id='storing', trigger_rule='none_failed_or_skipped')

	cleaning_xcoms = TriggerDagRunOperator(
		task_id='trigger_cleaning_xcoms',
		trigger_dag_id='cleaning_dag',
		execution_date=' {{ ds }}',
		wait_for_completion=True,
		poke_interval=60,
		reset_dag_run=True,
		failed_states=['failed'] 
		)

	# choosing_partner_based_on_day >> stop

	for partner, details in partners.items():
		@task.python(task_id=f'extract_{partner}', depends_on_past=True, priority_weight=details['priority'], 
			do_xcom_push=False, multiple_outputs=True,
			on_success_callback=_extract_callback_retry, on_failure_callback=_extract_callback_failure,
			on_retry_callback=_extract_callback_retry,
			retries=3, retry_delay=timedelta(minutes=5),
			sla=timedelta(minutes=5)
			)
		def extract(partner_name, partner_path):
			raise ValueError('failed')
			return {"partner_name": partner_name, "partner_path": partner_path}
		extracted_values = extract(details['name'], details['path'])
		start >> extracted_values
		process_tasks(extracted_values) >> storing

	
dag = my_dag()