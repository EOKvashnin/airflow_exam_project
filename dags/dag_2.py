from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "djeff_07",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 13),
    #"retry_delay": timedelta(minutes=0.1)
}
dag = DAG('Export_Weather_to_DB', default_args=default_args, schedule_interval='00 01,13 * * *', catchup=False,
          max_active_tasks=3, max_active_runs=1, tags=["Test", "My second dag"])

task_1 = BashOperator(
    task_id='task_1',
    bash_command='python3 /airflow/scripts/dag2/task_1.py',
    dag=dag)




