from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'email':'youremail@mail.com',
    'email_on_failure': False
}

my_dag = DAG(
    'primerate_dag',
    start_date=datetime(2023, 11, 7),
    schedule_interval='@daily',
    default_args=default_args
)

primerate_extract = BashOperator(
    task_id='primerate_extract', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/extraction_script/primerate_extraction.py', 
    dag=my_dag
)

primerate_transf = BashOperator(
    task_id='primerate_transf', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/transf_script/primerate_transf.py', 
    dag=my_dag
)

primerate_to_mysql = BashOperator(
    task_id='primerate_to_mysql', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/sql_script/primerate_to_mysql.py', 
    dag=my_dag
)

primerate_extract >> primerate_transf >> primerate_to_mysql