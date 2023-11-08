from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from datetime import datetime

default_args = {
    'email':'youremail@mail.com',
    'email_on_failure': False
}


my_dag = DAG(
    'currency_crypto_dag',
    start_date=datetime(2023, 11, 7),
    schedule_interval='@daily',
    default_args=default_args
)

currency_extract = BashOperator(
    task_id='currency_extract', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/extraction_script/currency_extraction.py', 
    dag=my_dag
)

crypto_extract = BashOperator(
    task_id='crypto_extract', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/extraction_script/crypto_extraction.py', 
    dag=my_dag
)

currency_transf = BashOperator(
    task_id='currency_transf', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/transf_script/currency_transf.py', 
    dag=my_dag
)

crypto_transf = BashOperator(
    task_id='crypto_transf', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/transf_script/crypto_transf.py', 
    dag=my_dag
)

currency_to_mysql = BashOperator(
    task_id='currency_to_mysql', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/sql_script/currency_to_mysql.py', 
    dag=my_dag
)

crypto_to_mysql = BashOperator(
    task_id='crypto_to_mysql', 
    bash_command='python3 /home/liperoc/projects/etl_finance_pyspark/scripts/sql_script/crypto_to_mysql.py', 
    dag=my_dag
)


chain([currency_extract, crypto_extract], [currency_transf, crypto_transf], [currency_to_mysql,crypto_to_mysql])


