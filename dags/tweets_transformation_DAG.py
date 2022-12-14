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
    sql_get_data_dl = "SELECT twitter_user, tweet, tweet_date FROM premierinjuries_twitter_weekly"

    # Fetch all data from table in Data Lake
    cursor_dl.execute(sql_get_data_dl)
    tweets = cursor_dl.fetchall()

    tweets = tweets[1:11]

    # ---------------------- Create table into Data Warehouse ----------------------
    # Data Warehouse Credentials
    pg_hook = PostgresHook(
        postgres_conn_id='tweets_warehouse_test',
        schema='tweets'
    )

    # Connect to Data Warehouse
    pg_conn_dl_1 = pg_hook.get_conn()
    cursor_dl_1 = pg_conn_dl_1.cursor()

    # Drop tweets_without_transformation Table
    sql_drop_table = "DROP TABLE IF EXISTS tweets_without_transformation;"

    # Create New tweets_without_transformation Table
    sql_create_table = "CREATE TABLE IF NOT EXISTS tweets_without_transformation (Twitter_User VARCHAR(255), Tweet VARCHAR(512), Tweet_Date VARCHAR(255))"

    # Insert data into tweets_without_transformation
    cursor_dl_1.execute(sql_drop_table)
    cursor_dl_1.execute(sql_create_table)
    for row in tweets:
        cursor_dl_1.execute('INSERT INTO tweets_without_transformation VALUES %s', (row,))
    pg_conn_dl_1.commit()

# 3. Tweets Transformation
def transform_tweets():
    # ---------------------- Connect and Get Data from Data Warehouse ----------------------
    # Data Warehouse Credentials
    pg_hook = PostgresHook(
        postgres_conn_id='tweets_warehouse_test',
        schema='tweets'
    )

    # Connect to Data Warehouse
    pg_conn_dw = pg_hook.get_conn()
    cursor_dw = pg_conn_dw.cursor()

    # Get the data from the Data Warehouse
    sql_get_data_dw = "SELECT * FROM tweets_without_transformation"

    # Fetch all data from Data Warehouse
    cursor_dw.execute(sql_get_data_dw)
    tweets = cursor_dw.fetchall()

    # ---------------------- Create DataFrame ----------------------
    # Define columns
    columns = ['Twitter_User', 'Tweet', 'Tweet_Date']
    # Create the DataFrame
    twitter_df = pd.DataFrame(tweets, columns=columns)

    # ---------------------- Data Transformation ----------------------
    # Replace missing values
    twitter_df = twitter_df.replace(['NA'], np.nan)


    # Getting a new column with the name of the injured player
    name_player = twitter_df['Tweet'].str.split('#FPL Update: ', n=1, expand=True)
    twitter_df['Full_Name_Player'] = name_player[1]

    name_player = twitter_df['Full_Name_Player'].str.split(' -', n=1, expand=True)
    twitter_df['Full_Name_Player'] = name_player[0]

    # Getting new columns with the first and second name of the injured player
    twitter_df['Full_Name_Player'] = twitter_df.full_name_player.str.strip()
    twitter_df[['First_name', 'Second_Name']] = twitter_df['Full_Name_Player'].str.split(' ', n=1, expand=True)


    # Getting a new column with the type of injury
    injury = twitter_df['Tweet'].str.split('- ', n=1, expand=True)
    twitter_df['Injury'] = injury[1]

    injury = twitter_df['Injury'].str.split(' #', n=1, expand=True)
    twitter_df['Injury'] = injury[0]


    # Getting a new column with the expected return date
    return_date = twitter_df['Tweet'].str.split('Expected Return: ', n=1, expand=True)
    twitter_df['Expected_Return_Date'] = return_date[1]

    return_date = twitter_df['Expected_Return_Date'].str.split(' ', n=1, expand=True)
    twitter_df['Expected_Return_Date'] = return_date[0]

    twitter_df['Expected_Return_Date'] = pd.to_datetime(twitter_df['Expected_Return_Date']).dt.date

    twitter_df['Expected_Return_Date'] = twitter_df['Expected_Return_Date'].fillna('No Return Date')


    # Getting a new column with status
    status = twitter_df['Tweet'].str.split('Status: ', n = 1, expand = True)
    twitter_df['Status'] = status[1]

    status = twitter_df['Status'].str.split(' ', n = 2, expand = True)
    twitter_df['Status'] = status[0]

    twitter_df['Status'] = twitter_df['Status'].replace('Ruled', 'Ruled Out', regex = True)

    print('Data successfully transformed')

    tweets_list = [tuple(x) for x in twitter_df.to_numpy()]

    # ---------------------- Load Transformed Data into Data Warehouse ----------------------
    # Drop transformed_tweets Table
    sql_drop_table = "DROP TABLE IF EXISTS weekly_tweets;"

    # Create New tweets_without_transformation Table
    sql_create_table = "CREATE TABLE IF NOT EXISTS weekly_tweets (Twitter_User VARCHAR(255), Tweet VARCHAR(512),\
                        Tweet_Date VARCHAR(255), Full_Name_Player VARCHAR(255), First_Name VARCHAR(255), \
                        Second_Name VARCHAR(255), Injury VARCHAR(255), Expected_Return_Date VARCHAR(255),\
                        Status VARCHAR(255))"

    # Execute SQL statements
    cursor_dw.execute(sql_drop_table)
    cursor_dw.execute(sql_create_table)

    # Commit
    pg_conn_dw.commit()

    # Insert data into Data Lake
    pg_hook.insert_rows(table="weekly_tweets", rows=tweets_list)

# 4. Log the end of the DAG
def finish_dag():
    logging.info('Ending the DAG. Obtained and transformed tweets from @PremierInjuries')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('transform_tweets_DAG',
          schedule_interval='0 0 * * *',
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

# 3. Transformation_Task
data_transformation = PythonOperator(
    task_id = "data_transformation",
    python_callable = transform_tweets,
    dag = dag
)

# 4. End Task
finish_task = PythonOperator(
    task_id = "finish_task",
    python_callable = finish_dag,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------
start_task >> table_dw >> data_transformation >> finish_task