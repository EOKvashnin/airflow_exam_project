from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook

connection = BaseHook.get_connection("main_postgresql_connection")


default_args = {
    "owner": "djeff_07",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 14),
    #"retry_delay": timedelta(minutes=0.1)
}
dag = DAG('API_wthr_to_DB_AIRFLW_default_conn', default_args=default_args, schedule_interval=None, catchup=False,
          max_active_tasks=3, max_active_runs=1, tags=["Test", "My second dag"])

task1 = BashOperator(
    task_id='task1',
    bash_command='python3 /airflow/scripts/dag4/task_1.py --date {{ ds }} ' +f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5432',
    dag=dag)

