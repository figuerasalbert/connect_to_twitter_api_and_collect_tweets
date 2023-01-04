# ----------------------------- Load Packages -----------------------------
# For DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For Transformation
import pandas as pd
from unidecode import unidecode

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
    columns = ['id_tweet', 'twitter_user', 'tweet', 'tweet_date', 'player', 'first_name', 'second_name',
               'team', 'injury', 'expected_return_date', 'status']

    tweets_df = pd.DataFrame(tuples_list_1, columns=columns)


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
                             "team, code FROM stage_elements;"

    # Fetch data
    cursor_2.execute(sql_statement_get_data)
    tuples_list_2 = cursor_2.fetchall()

    # Create DataFrame
    column_names = ['first_name', 'second_name', 'web_name', 'team', 'code']

    elements_df = pd.DataFrame(tuples_list_2, columns=column_names)

    # Transform elements_df; no accents
    elements_df['first_name'] = elements_df['first_name'].apply(unidecode)
    elements_df['second_name'] = elements_df['second_name'].apply(unidecode)

    # Join 1 - based on first and second name
    df = pd.merge(tweets_df, elements_df[['first_name', 'second_name', 'team', 'code']],
                  on=['first_name', 'second_name', 'team'], how='left')

    # Joined successfully
    assigned = df[df['code'].notnull()]

    # Joined unsuccessfully
    missing = df[df['code'].isnull()]
    missing.pop("code")


    # Join 2 - based on player, web_name and team
    missing = pd.merge(tweets_df, elements_df[['web_name', 'team', 'code']],
                       left_on=['player', 'team'],
                       right_on=['web_name', 'team'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop("code")


    # Join 3 - based on first_name, web_name and team
    missing = pd.merge(missing, elements_df[['web_name', 'team', 'code']],
                       left_on=['first_name', 'team'],
                       right_on=['web_name', 'team'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop("code")


    # Join 4 - based on second_name, web_name and team
    missing = pd.merge(missing, elements_df[['web_name', 'team', 'code']],
                       left_on=['second_name', 'team'],
                       right_on=['web_name', 'team'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop("code")

    # Join 5 - based only on first and second name, not team because a player could have changed of club
    missing = pd.merge(missing, elements_df[['first_name', 'second_name', 'code']],
                       left_on=['first_name', 'second_name'],
                       right_on=['first_name', 'second_name'],
                       how='left')

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop('code')

    # Join 6 based only on first_name and web_name, not club
    missing = pd.merge(missing, elements_df[['web_name', 'code']],
                       left_on=['first_name'],
                       right_on=['web_name'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop('code')

    # Join 7 - based only on second_name and web_name, not club because a player could have changed of club
    missing = pd.merge(missing, elements_df[['web_name', 'code']],
                       left_on=['second_name'],
                       right_on=['web_name'],
                       how='left')

    missing.pop("web_name")

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])

    # Joined unsuccessfully
    missing = missing[missing['code'].isnull()]
    missing.pop('code')

    # Join 8 - based only on player and web_name, not club because a player could have changed of club
    missing = pd.merge(missing, elements_df[['web_name', 'code']],
                       left_on=['player'],
                       right_on=['web_name'],
                       how='left')
    missing.pop('web_name')

    # Add new successful joins to assigned
    assigned = pd.concat([assigned, missing[missing['code'].notnull()]])
    assigned['code'] = pd.to_numeric(assigned['code'])
    assigned['code'] = assigned['code'].astype('int')

    # Remove duplicate rows
    assigned = assigned.drop_duplicates()

    #Change the order of the columns data frame
    assigned = assigned[['code', 'id_tweet', 'twitter_user', 'tweet', 'tweet_date', 'player', 'first_name',
                         'second_name', 'team', 'injury', 'expected_return_date', 'status']]


    # ---------------------- Load data into the Data Warehouse ----------------------
    # Drop weekly_tweets Table
    sql_drop_table_1 = "DROP TABLE IF EXISTS weekly_tweets_code;"

    # Create New weekly_tweets Table
    sql_create_table_1 = "CREATE TABLE IF NOT EXISTS weekly_tweets_code (code INT, id_tweet INT, twitter_user VARCHAR(255)," \
                         "tweet VARCHAR(512), tweet_date VARCHAR(255), player VARCHAR(255), first_name VARCHAR(255)," \
                         "second_name VARCHAR(255), team VARCHAR(255), injury VARCHAR(255)," \
                         "expected_return_date VARCHAR(255), status VARCHAR(255))"

    # Drop and Create staging table
    cursor_1.execute(sql_drop_table_1)
    cursor_1.execute(sql_create_table_1)
    pg_conn_1.commit()

    # Create a list of tuples representing the rows in the dataframe
    rows = [tuple(x) for x in assigned.values]

    # Insert the rows into the database
    pg_hook_1.insert_rows(table="weekly_tweets_code", rows=rows)

# 3. Log the end of the DAG
def finish_dag():
    logging.info("Ending the DAG. Tweets are loaded into 'datawarehouse' database")

# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('task_03_assign_fpl_players_id',
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