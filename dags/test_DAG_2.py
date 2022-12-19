# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Transformation
import pandas as pd
import numpy as np

# Connecting to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------
# 1. Log the start of the DAG
def start_dag():
    logging.info("Starting the DAG. Assigning id players from Fantasy Premier League")


# 2. Collect data from 'tweets' database
def set_id():
    ################ Get injuries data from DW

    # Data warehouse: injuries
    pg_hook_1 = PostgresHook(
        postgres_conn_id='tweets_warehouse',
        schema='tweets'
    )
    # Connect to data warehouse: injuries
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement: Get data
    sql_get_data = "SELECT * FROM weekly_tweets;"

    # Fetch data
    cursor_1.execute(sql_get_data)
    tuples_list_1 = cursor_1.fetchall()

    # Create DataFrame
    column_names = ['id_tweet', 'Twitter_User', 'Tweet', 'Tweet_Date', 'Player', 'First_Name', 'Second_Name',
               'Team', 'Injury', 'Expected_Return_Date', 'Status']

    df1 = pd.DataFrame(tuples_list_1, columns=column_names)

    ################ Get fpl data from DW

    # Data warehouse: fpl
    pg_hook_2 = PostgresHook(
        postgres_conn_id='fantasypl_warehouse',
        schema='fantasypl'
    )
    # Connect to data warehouse: fpl
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statement: Get data
    sql_statement_get_data = "SELECT first_name, second_name, web_name, " \
                             "team, code FROM elements;"

    # Fetch data
    cursor_2.execute(sql_statement_get_data)
    tuples_list_2 = cursor_2.fetchall()

    # Create DataFrame
    column_names = ['first_name', 'second_name', 'web_name', 'team', 'code']

    df2 = pd.DataFrame(tuples_list_2, columns=column_names)

    # Transform elements_df; no accents
    df2['first_name'] = df2['first_name'].apply(unidecode)
    df2['second_name'] = df2['second_name'].apply(unidecode)

    # Join 1 - based on first and second name
    df = pd.merge(df1, df2[['first_name', 'second_name', 'team', 'code']],
                  on=['first_name', 'second_name', 'team'], how='left')

    # Joined successfully
    assigned = df[df['code'].notnull()]

    # Joined unsuccessfully
    missing = df[df['code'].isnull()]
    missing.pop("code")
    print('Join 1 successful')

    # ---------------------- Load data into the Data Warehouse ----------------------
    # Drop weekly_tweets Table
    sql_drop_table_1 = "DROP TABLE IF EXISTS weekly_tweets_code;"

    # Create New weekly_tweets Table
    sql_create_table_1 = "CREATE TABLE IF NOT EXISTS weekly_tweets_code (id_tweet INT, Twitter_User VARCHAR(255)," \
                         "Tweet VARCHAR(512), Tweet_Date VARCHAR(255), Player VARCHAR(255), First_Name VARCHAR(255)," \
                         "Second_Name VARCHAR(255), Team VARCHAR(255), Injury VARCHAR(255)," \
                         "Expected_Return_Date VARCHAR(255), Status VARCHAR(255), code INT)"

    # Drop and Create staging table
    cursor_1.execute(sql_drop_stage)
    cursor_1.execute(sql_statement_create_table)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in assigned.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="stage_clean_historical_injuries", rows=rows)

# 3. Log the end of the DAG
def finish_dag():
    logging.info("Ending the DAG. Tweets are loaded into 'datawarehouse' database")

# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('TEST_02_DAG_id_players',
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

# 2. Assign fpl_player_id to injured players from @PremierInjuries
assign_id_player_dag = PythonOperator(
    task_id = 'assign_id_player_dag',
    python_callable = set_id,
    dag = dag
)

# 3. End Task
finish_task = PythonOperator(
    task_id = "finish_task",
    python_callable = finish_dag,
    dag = dag
)


# ----------------------------- Trigger Tasks -----------------------------
start_task >> assign_id_player_dag >> finish_task