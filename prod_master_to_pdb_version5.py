from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import pymysql
import mysql.connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'prod_master_to_pdb_version5',
    default_args=default_args,
    description='A simple DAG to delete all rows in a MySQL table on Wednesdays at 3:30 AM',
    schedule_interval='30 3 * * *', # schedule on Wednesdays at 3:30 AM
    catchup=False
)

delete_all_rows = MySqlOperator(
    task_id='delete_all_rows',
    mysql_conn_id='pricing_db_credentials',
    sql='DELETE FROM prod_master',
    dag=dag,
)           #task1

into_a_dataframe = BashOperator(
    task_id="into_a_dataframe",
    bash_command='mysql -h {host} -u {user} -p{password} {database} -e "SELECT * FROM {table};" > /mnt/c/DAG/mysql_output.csv'.format(
        host='host_does_not_exist',
        user='me',
        password='pass',
        database='niyoweb',
        table='Prod_Master'
    ),
    dag=dag
)

def read_csv_data():
    df= pd.read_csv('/mnt/c/DAG/mysql_output.csv')
    conn = mysql.connector.connect(
        host='where_to_load',
        user='me',
        database='pass'
    )

    result = df.to_sql('prod_master',con=conn,if_exists="append",index=False)
    print(result)

    conn.close

delete_all_rows >> into_a_dataframe