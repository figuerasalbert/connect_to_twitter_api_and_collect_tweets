# ----------------------------- Import Required Packages -----------------------------
# Packages for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Packages for Twitter API
import pandas as pd


# Packages for Connect to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook


# ----------------------------- Define Functions -----------------------------
# 1. Log the start of the DAG
def start_dag():
    logging.info("Starting the DAG. Loading the data into 'datawarehouse' database)

# 2. Collect data from 'tweets' database
def tweets_db():
    # ---------------------- Connect to Data Warehouse and get data from tweets database ----------------------
    # Data Warehouse Credentials
    pg_hook = PostgresHook(
        postgres_conn_id='tweets_warehouse',
        schema='tweets'
    )

    # Connect to Data Warehouse
    pg_conn_dw = pg_hook.get_conn()
    cursor_dw = pg_conn_dw.cursor()

    # Get the data from the Data Warehouse
    sql_get_data_dw = "SELECT * FROM weekly_tweets"

    # Fetch all data from Data Warehouse
    cursor_dw.execute(sql_get_data_dw)
    tweets_list = cursor_dw.fetchall()

    return tweets_list


# 3. Load data into 'datawarehouse' database
def datawarehouse_db(ti):
    # ---------------------- Get the data from the previous task ----------------------
    # Get the data from the previous task
    data = ti.xcom_pull(task_ids = ['get_tweets_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Extract data from nested list
    tweets_data = data[0]

    # ---------------------- Connect to Data Warehouse ----------------------
    # Data Warehouse Credentials
    pg_hook = PostgresHook(
        postgres_conn_id='tweets_warehouse',
        schema='datawarehouse'
    )

    # Connect to Data Warehouse
    pg_conn_dw_1 = pg_hook.get_conn()
    cursor_dw_1 = pg_conn_dw_1.cursor()

    # ---------------------- Load the data into datawarehouse db ----------------------
    # Drop transformed_tweets Table
    sql_drop_table = "DROP TABLE IF EXISTS weekly_tweets;"

    # Create New tweets_without_transformation Table
    sql_create_table = "CREATE TABLE IF NOT EXISTS weekly_tweets (Twitter_User VARCHAR(255), Tweet VARCHAR(512),\
                            Tweet_Date VARCHAR(255), Player VARCHAR(255), Injury VARCHAR(255), Expected_Return_Date VARCHAR(255),\
                            Status VARCHAR(255))"

    # Execute SQL statements
    cursor_dw_1.execute(sql_drop_table)
    cursor_dw_1.execute(sql_create_table)

    # Commit
    pg_conn_dw_1.commit()

    # Insert data into Data Lake
    pg_hook.insert_rows(table="weekly_tweets", rows=tweets_data)


# 4. Log the end of the DAG
def finish_dag():
    logging.info("Ending the DAG. Tweets are loaded into 'datawarehouse' database")


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('load_data_datawarehouse_db',
          schedule_interval='10 0 * * *',
          catchup=False,
          default_args=default_args)


# ----------------------------- Set DAG Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_dag,
    dag = dag
)


# 2. Collect data from 'tweets' database
tweets_db_dag = PythonOperator(
    task_id = 'tweets_db_task',
    python_callable = tweets_db,
    do_xcom_push = True,
    dag = dag
)

# 3. Load data into 'datawarehouse' database
datawarehouse_db_dag = PythonOperator(
    task_id = 'datawarehouse_db_task',
    python_callable = datawarehouse_db,
    do_xcom_push = True,
    dag = dag
)

# 4. End Task
finish_task = PythonOperator(
    task_id = "finish_task",
    python_callable = finish_dag,
    dag = dag
)


# ----------------------------- Trigger Tasks -----------------------------
start_task >> tweets_db_dag >> datawarehouse_db_dag >> finish_task
