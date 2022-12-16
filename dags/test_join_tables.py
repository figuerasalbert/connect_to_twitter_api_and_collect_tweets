# ----------------------------- Import Required Packages -----------------------------
# Packages for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Packages for Twitter API
import pandas as pd
import numpy as np

# Packages for Connect to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook


# ----------------------------- Define Functions -----------------------------
# 1. Log the start of the DAG
def start_dag():
    logging.info('Starting the DAG. Obtaining data from Data Lake')

# 2. Create new table into Data Warehouse
def table_dw():
    # ---------------------- Connect and Get data from Data Lake ----------------------
    # Data Lake Credentials
    pg_hook = PostgresHook(
        postgres_conn_id='twitter_api_airflow',
        schema='twitter_api'
    )

    # Connect to Data Lake
    pg_conn_dl = pg_hook.get_conn()
    cursor_dl = pg_conn_dl.cursor()

    # Get data from Data Lake
    sql_get_data_dl = "SELECT * FROM premierinjuries_twitter_historicaL"

    # Fetch all data from table in Data Lake
    cursor_dl.execute(sql_get_data_dl)
    tweets = cursor_dl.fetchall()


    # ---------------------- Create table into Data Warehouse ----------------------
    # Data Warehouse Credentials
    pg_hook = PostgresHook(
        postgres_conn_id='tweets_warehouse',
        schema='tweets'
    )

    # Connect to Data Warehouse
    pg_conn_dw = pg_hook.get_conn()
    cursor_dw = pg_conn_dw.cursor()

    # Drop tweets_without_transformation Table
    sql_drop_table = "DROP TABLE IF EXISTS tweets_without_transformation_historical;"

    # Create New tweets_without_transformation Table
    sql_create_table = "CREATE TABLE IF NOT EXISTS tweets_without_transformation_historical (Twitter_User VARCHAR(255), Tweet VARCHAR(512),\
                Tweet_Date VARCHAR(255), Player VARCHAR(255), First_Name VARCHAR(255), Second_Name VARCHAR(255),\
                Club VARCHAR(255), Injury VARCHAR(255), Expected_Return_Date VARCHAR(255), Status VARCHAR(255))"

    # Insert data into tweets_without_transformation
    cursor_dw.execute(sql_drop_table)
    cursor_dw.execute(sql_create_table)
    for row in tweets:
        cursor_dw.execute('INSERT INTO tweets_without_transformation_historical VALUES %s', (row,))
    pg_conn_dw.commit()

# 4. Log the end of the DAG
def finish_dag():
    logging.info('Ending the DAG. Obtained and transformed tweets from @PremierInjuries')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('test_historical',
          schedule_interval='5 0 * * *',
          catchup=False,
          default_args=default_args)

# ----------------------------- Set DAG Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_dag,
    dag = dag
)

# 2. Create new table into Data Warehouse
table_dw = PythonOperator(
    task_id = "table_dw",
    python_callable = table_dw,
    dag = dag
)


# 4. End Task
finish_task = PythonOperator(
    task_id = "finish_task",
    python_callable = finish_dag,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------
start_task >> table_dw >> finish_task