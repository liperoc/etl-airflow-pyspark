from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'email':'youremail@mail.com',
    'email_on_failure': False
}

my_dag = DAG(
    'inflation_dag',
    start_date=datetime(2023, 11, 7),
    schedule_interval='@monthly',
    default_args=default_args
)

inflation_extract = BashOperator(
    task_id='inflation_extract', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/extraction_script/inflation_extraction.py', 
    dag=my_dag
)

inflation_transf = BashOperator(
    task_id='inflation_transf', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/transf_script/inflation_transf.py', 
    dag=my_dag
)

inflation_to_mysql = BashOperator(
    task_id='inflation_to_mysql', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/sql_script/inflation_to_mysql.py', 
    dag=my_dag
)

inflation_extract >> inflation_transf >> inflation_to_mysql