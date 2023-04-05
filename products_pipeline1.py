from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.base_hook import BaseHook
import sqlalchemy 
import io
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
    'products_to_pdb1',
    default_args=default_args,
    description='A simple DAG to delete all rows in a MySQL table on Wednesdays at 3:30 AM',
    schedule_interval='30 3 * * *', 
    catchup=False
)



def query_mysql_table():
    mysql_hook = MySqlHook(mysql_conn_id='conn_id_here')
    conn = mysql_hook.get_conn()
    df = pd.read_sql("SELECT * FROM products LIMIT 10", con=conn)
    
    df1=df.drop(["localization"], axis=1)
    
    print(df1)
    #df1.to_csv("/mnt/c/DAG/output1.csv")
    conn.close()


    host='host_does_not_exist'
    user='lalit.joshi'
    password='pass'
    database = 'db_name'
    port = 3306
    
    conn1 = sqlalchemy.create_engine("mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))


    df1.to_sql('products',con=conn1,if_exists="append",index=False)
    print("done")



with dag:
    t1 = PythonOperator(
        task_id='query_mysql_table',
        python_callable=query_mysql_table
    )

    t1 #>> t2

