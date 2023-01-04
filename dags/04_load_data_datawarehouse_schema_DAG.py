# ----------------------------- Import Required Packages -----------------------------
# Packages for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Packages for Connect to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook


# ----------------------------- Define Functions -----------------------------
# 1. Log the start of the DAG
def start_dag():
    logging.info("Starting the DAG. Loading the data into 'datawarehouse' database")

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
    sql_get_data_dw = "SELECT * FROM weekly_tweets_code"

    # Fetch all data from Data Warehouse
    cursor_dw.execute(sql_get_data_dw)
    tweets_list = cursor_dw.fetchall()

    return tweets_list


# 3. Load data into 'datawarehouse' database
def datawarehouse_db(ti):
    # ---------------------- Get the data from the previous task ----------------------
    # Get the data from the previous task
    data = ti.xcom_pull(task_ids = ['tweets_db_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Extract data from nested list
    tweets_data = data[0]

    # ---------------------- Connect to Data Warehouse ----------------------
    # Data Warehouse Credentials
    pg_hook = PostgresHook(
        postgres_conn_id='tweets_warehouse',
        schema='dw_injuries'
    )

    # Connect to Data Warehouse
    pg_conn_dw_1 = pg_hook.get_conn()
    cursor_dw_1 = pg_conn_dw_1.cursor()

    # ---------------------- Load the data into datawarehouse db ----------------------
    # Drop transformed_tweets Table
    sql_drop_table = "DROP TABLE IF EXISTS weekly_tweets;"

    # Create New weekly Table
    sql_create_table = "CREATE TABLE IF NOT EXISTS weekly_tweets (code INT," \
                        "id_tweet INT PRIMARY KEY, twitter_user VARCHAR(255)," \
                         "tweet VARCHAR(512), tweet_date VARCHAR(255), player VARCHAR(255), first_name VARCHAR(255)," \
                         "second_name VARCHAR(255), team VARCHAR(255), injury VARCHAR(255)," \
                         "expected_return_date VARCHAR(255), status VARCHAR(255))"


    # Execute SQL statements
    cursor_dw_1.execute(sql_drop_table)
    cursor_dw_1.execute(sql_create_table)

    # Commit
    pg_conn_dw_1.commit()

    # Insert data into Data Lake
    pg_hook.insert_rows(table="weekly_tweets", rows=tweets_data)


# 4. DROP table weekly_tweets_code from the db tweets
def drop_table():
    # ---------------------- Connect to Data Warehouse and get data from tweets database ----------------------
    # Data Warehouse Credentials
    pg_hook3 = PostgresHook(
        postgres_conn_id='tweets_warehouse',
        schema='tweets'
    )

    # Connect to Data Warehouse
    pg_conn_dw3 = pg_hook3.get_conn()
    cursor_dw3 = pg_conn_dw3.cursor()

    # Drop the table from the data base tweets
    sql_drop_table = "DROP TABLE weekly_tweets_code;"

    # Fetch all data from Data Warehouse
    cursor_dw3.execute(sql_drop_table)
    # Commit
    pg_conn_dw3.commit()


# 5. Log the end of the DAG
def finish_dag():
    logging.info("Ending the DAG. Tweets are loaded into 'datawarehouse' database")


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('task_04_load_data_datawarehouse_db',
          schedule_interval='20 0 * * *',
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
tweets_db_task = PythonOperator(
    task_id = 'tweets_db_task',
    python_callable = tweets_db,
    do_xcom_push = True,
    dag = dag
)

# 3. Load data into 'datawarehouse' database
datawarehouse_db_task = PythonOperator(
    task_id = 'datawarehouse_db_task',
    python_callable = datawarehouse_db,
    do_xcom_push = True,
    dag = dag
)

# 4. Drop table
drop_table_task = PythonOperator(
    task_id = 'drop_table_task',
    python_callable = drop_table,
    dag = dag
)


# 5. End Task
finish_task = PythonOperator(
    task_id = "finish_task",
    python_callable = finish_dag,
    dag = dag
)


# ----------------------------- Trigger Tasks -----------------------------
start_task >> tweets_db_task >> datawarehouse_db_task >> drop_table_task >> finish_task
