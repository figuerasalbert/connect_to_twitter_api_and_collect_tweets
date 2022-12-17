# Packages for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Packages for the data transforming
import pandas as pd
from unidecode import unidecode


# Packages for Connect to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook

# ----------------------------- Define Functions -----------------------------
# 1. Log the start of the DAG
def start_dag():
    logging.info("Starting the DAG. Assigning id players from Fantasy Premier League")


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

# 3. Assign fpl_player_id to injured players from @PremierInjuries
def set_id(ti):
    # ---------------------- Get the data from the previous task ----------------------
    # Get the data from the previous task
    data = ti.xcom_pull(task_ids = ['tweets_db_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Extract data from nested list
    tweets_data = data[0]

    # ---------------------- Get the data from the previous task ----------------------
    # Get the data from the previous task
    data = ti.xcom_pull(task_ids = ['tweets_db_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Extract data from nested list
    tweets_data = data[0]

    # Create a DataFrame
    columns = ['id_tweet','Twitter_User', 'Tweet', 'Tweet_Date', 'Player', 'First_Name', 'Second_Name',
               'Team', 'Injury', 'Expected_Return_Date', 'Status']

    tweets_df = pd.DataFrame(tweets_data, columns = columns)

    # ---------------------- Get the data from the fantasypl db ----------------------
    # Data Warehouse Credentials
    pg_hook_2 = PostgresHook(
        postgres_conn_id='fantasypl_warehouse',
        schema='fantasypl'
    )
    # Connect to Data Warehouse
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # SQL Statement: Get data
    sql_get_data = "SELECT first_name, second_name, web_name, " \
                             "team, code FROM elements;"

    # Fetch data
    cursor_2.execute(sql_get_data)
    elements = cursor_2.fetchall()

    # Create DataFrame
    columns = ['first_name', 'second_name', 'web_name', 'team', 'code']

    elements_df = pd.DataFrame(elements, columns=columns)

    # Ignore accents for first_name and second_name
    elements_df['first_name'] = elements_df['first_name'].apply(unidecode)
    elements_df['second_name'] = elements_df['second_name'].apply(unidecode)

    # ---------------------- Join tweets_df with elements_df, considering fpl player id ----------------------
    # Join 1 - based on first and second name, and team
    df = pd.merge(tweets_df, elements_df[['first_name', 'second_name', 'team', 'code']],
                  left_on=['First_Name', 'Second_Name', 'Team'],
                  right_on=['first_name', 'second_name', 'team'],
                  how='left')

    # Joined successfully
    assigned = df[df['code'].notnull()]

    # Joined unsuccessfully
    missing = df[df['code'].isnull()]
    missing.pop("code")

    print('Join 1 succesfull')

    # Join 2 - based on player, web_name and team
    missing = pd.merge(missing, elements_df[['web_name', 'team', 'code']],
                       left_on=['Player', 'Team'],
                       right_on=['web_name', 'team'],
                       how='left')

    missing.pop("web_name")


    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop("code")
    print('Join 2 successful')

    # Join 3 - based on first_name, web_name and team
    missing = pd.merge(missing, elements_df[['web_name', 'team', 'code']],
                       left_on=['First_Name', 'Team'],
                       right_on=['web_name', 'team'],
                       how='left')

    missing.pop("web_name")


    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop("code")
    print('Join 3 successful')

    #df = df.reset_index()

    # Join 4 - based on second_name, web_name and team
    #missing = pd.merge(missing, elements_df[['web_name', 'team', 'code']],
                       #left_on=['Second_Name', 'Team'],
                       #right_on=['web_name', 'team'],
                       #how='left')

    #missing.pop("web_name")


    # Add new successful joins to assigned
    #assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    #missing = missing[missing['code'].isnull()]
    #missing.pop("code")
    #print('Join 4 successful')
    #df = df.reset_index()

    # Join 5 - based only on first and second name, not team because a player could have changed of club
    missing = pd.merge(missing, elements_df[['first_name', 'second_name', 'code']],
                       left_on=['First_Name', 'Second_Name'],
                       right_on=['first_name', 'second_name'],
                       how='left')


    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop('code')

    print('Join 5 successful')
    df = df.reset_index()

    # Join 6 - based only on first_name and web_name, not club because a player could have changed of club
    missing = pd.merge(missing, elements_df[['web_name', 'code']],
                       left_on=['First_Name'],
                       right_on=['web_name'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop('code')

    print('Join 6 successful')

    # Join 7 - based only on second_name and web_name, not club because a player could have changed of club
    missing = pd.merge(missing, elements_df[['web_name', 'code']],
                       left_on=['Second_Name'],
                       right_on=['web_name'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop('code')
    print('Join 7 successful')

    # Join 8 - based only on player and web_name, not club because a player could have changed of club
    missing = pd.merge(missing, elements_df[['web_name', 'code']],
                       left_on=['Player'],
                       right_on=['web_name'],
                       how='left')
    missing.pop('web_name')


    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]

    print('Join 8 successful')

    ################ Load data to staging table

    # Data Warehouse Credentials
    pg_hook = PostgresHook(
        postgres_conn_id='tweets_warehouse',
        schema='tweets'
    )

    # Connect to Data Warehouse
    pg_conn_dw = pg_hook.get_conn()
    cursor_dw = pg_conn_dw.cursor()

    # SQL Statements: Drop and create staging table
    sql_drop_table = "DROP TABLE IF EXISTS weekly_tweets;"
    sql_create_table = "CREATE TABLE IF NOT EXISTS weekly_tweets (id_tweet INT, Twitter_User VARCHAR(255)," \
                        "Tweet VARCHAR(512), Tweet_Date VARCHAR(255), Player VARCHAR(255), First_Name VARCHAR(255),"\
                        "Second_Name VARCHAR(255), Team VARCHAR(255), Injury VARCHAR(255),"\
                        "Expected_Return_Date VARCHAR(255), Status VARCHAR(255), code INT);"

    # Drop and Create staging table
    cursor_dw.execute(sql_drop_table)
    cursor_dw.execute(sql_create_table)
    pg_conn_dw.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in assigned.values]

    # Insert the rows into the database
    pg_hook.insert_rows(table="weekly_tweets", rows=rows)

# 4. Log the end of the DAG
def finish_dag():
    logging.info("Ending the DAG. Tweets are loaded into 'datawarehouse' database")

# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('test_DAG_id_players',
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
    dag = dag
)

# 3. Assign fpl_player_id to injured players from @PremierInjuries
assign_id_player_dag = PythonOperator(
    task_id = 'assign_id_player_dag',
    python_callable = set_id,
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
start_task >> tweets_db_dag >> assign_id_player_dag >> finish_task