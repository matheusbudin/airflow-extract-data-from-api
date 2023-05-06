import pendulum
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'start_date': datetime(2023, 5, 2, tzinfo=pendulum.timezone("Europe/Berlin")),
    'email': ['dev.matheusbudin@gmail.com'],
    'email_on_failure': False
}


dag = DAG(
    dag_id = 'hello-world',
    default_args=args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

task_hello_world = BashOperator(
    task_id = 'task_hello_world',
    bash_command='echo "hello world"',
    dag = dag,
)