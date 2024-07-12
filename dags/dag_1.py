from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "djeff_07",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 8),
    #"retry_delay": timedelta(minutes=0.1)
}
#'*/15 * * * *'
dag = DAG('dag1', default_args=default_args, schedule_interval=None, catchup=False,
          max_active_tasks=3, max_active_runs=1, tags=["Test", "My first dagishe"])

# dag = DAG('dag1', default_args=default_args, schedule_interval='0 /15 * * *', catchup=False,
#           max_active_tasks=3, max_active_runs=1, tags=["Test", "My first dagishe"])

task_1 = BashOperator(
    task_id='task_1',
    bash_command='python3 /airflow/scripts/dag1/task_1.py',
    dag=dag)

task_2 = BashOperator(
    task_id='task_2',
    bash_command='python3 /airflow/scripts/dag1/task_2.py',
    dag=dag)

task_3 = BashOperator(
    task_id='task_3',
    bash_command='python3 /airflow/scripts/dag1/task_3.py',
    dag=dag)

task_1 >> task_2 >> task_3
