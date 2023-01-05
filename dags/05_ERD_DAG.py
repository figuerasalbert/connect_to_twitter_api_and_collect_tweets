# ----------------------------- Import Required Packages -----------------------------
# Packages for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# For work on Data Frames
import pandas as pd

# Packages for Connect to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook


# ----------------------------- Define Functions -----------------------------
# 1. Log the start of the DAG
def start_dag():
    logging.info('Starting the DAG. Creating the ERD')


# 2. Connect to the Data Warehouse and collect data from fantasypl db
def fantasypl_db():
    # ---------------------- Get code players data from DW ----------------------
    # Data warehouse: fantasypl
    pg_hook_1 = PostgresHook(
        postgres_conn_id='fantasypl_warehouse',
        schema='fantasypl'
    )
    # Connect to data warehouse: fantasypl
    pg_conn_1 = pg_hook_1.get_conn()
    cursor_1 = pg_conn_1.cursor()

    # SQL Statement: Get data
    sql_get_data = "SELECT code, first_name, second_name, web_name FROM store_gameweeks;"

    # Fetch data
    cursor_1.execute(sql_get_data)
    tuples_list_1 = cursor_1.fetchall()

    # Create DataFrame
    column_names = ['code', 'first_name', 'second_name', 'web_name']

    code_df = pd.DataFrame(tuples_list_1, columns=column_names)

    # Delete duplicate rows
    code_df = code_df.drop_duplicates()

    # Convert df to list
    code_list = [tuple(x) for x in code_df.to_numpy()]

    return code_list


# 3. Connect to the Data Warehouse and save the previous data to dw_injuries db
def dw_injuries_db(ti):
    # ---------------------- Get the data from the previous task ----------------------
    data = ti.xcom_pull(task_ids=['get_code_player_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Extract twitter data from nested list
    code_player = data[0]

    # ---------------------- Create a new table to save the data in the DW ----------------------
    # Data warehouse: dw_injuries
    pg_hook_2 = PostgresHook(
        postgres_conn_id='fantasypl_warehouse',
        schema='dw_injuries'
    )
    # Connect to data warehouse: dw_injuries
    pg_conn_2 = pg_hook_2.get_conn()
    cursor_2 = pg_conn_2.cursor()

    # Delete all records from table player_code
    sql_delete_records = "DELETE FROM player_code;"

    # Create New tweets_without_transformation Table
    sql_create_table = "CREATE TABLE IF NOT EXISTS player_code (code INT PRIMARY KEY, first_name VARCHAR(255)," \
                        "second_name VARCHAR(255), web_name VARCHAR(255));"


    # Execute SQL statements
    cursor_2.execute(sql_drop_table)
    cursor_2.execute(sql_create_table)

    # Commit
    pg_conn_2.commit()

    # Insert data into Data Lake
    pg_hook_2.insert_rows(table="player_code", rows = code_player)


# 4. Assign Foreign Keys to connect the other tables with player_code table
def assign_fk():
    # ---------------------- Connect to the DW ----------------------
    # Data warehouse: dw_injuries
    pg_hook_3 = PostgresHook(
        postgres_conn_id='fantasypl_warehouse',
        schema='dw_injuries'
    )
    # Connect to data warehouse: dw_injuries
    pg_conn_3 = pg_hook_3.get_conn()
    cursor_3 = pg_conn_3.cursor()

    # ---------------------- Assign Foreign Keys ----------------------
    # Alter table weekly_tweets to assign the foreign key
    sql_alter_table_weekly_tweets = "ALTER TABLE weekly_tweets ADD FOREIGN KEY (code) REFERENCES player_code(code)"
    sql_alter_table_store_historical_injuries = "ALTER TABLE store_historical_injuries ADD FOREIGN KEY (code) REFERENCES player_code(code)"

    # Execute SQL statement
    cursor_3.execute(sql_alter_table_weekly_tweets)
    cursor_3.execute(sql_alter_table_store_historical_injuries)

    # Commit
    pg_conn_3.commit()

# 5. Log the end of the DAG
def finish_dag():
    logging.info('Ending the DAG. ERD created')

# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('task_05_create_ERD',
          schedule_interval='0 7 * * *',
          catchup=False,
          default_args=default_args)

# ----------------------------- Set DAG Tasks -----------------------------
# 1. Start Task
start_task = PythonOperator(
    task_id = "start_task",
    python_callable = start_dag,
    dag = dag
)

# 2. API Connection and Get Tweets
get_code_player_task = PythonOperator(
    task_id = "get_code_player_task",
    python_callable = fantasypl_db,
    do_xcom_push = True,
    dag = dag
)

# 3. Load to Data Lake
save_code_player_task = PythonOperator(
    task_id = "save_code_player_task",
    python_callable = dw_injuries_db,
    do_xcom_push = True,
    dag = dag
)

# 4. Assign Foreign Keys
assign_fk_task = PythonOperator(
    task_id = "assign_fk_task",
    python_callable = assign_fk,
    dag = dag
)

# 5. End Task
finish_task = PythonOperator(
    task_id = "finish_task",
    python_callable = finish_dag,
    dag = dag
)

# ----------------------------- Trigger Tasks -----------------------------
start_task >> get_code_player_task >> save_code_player_task >> assign_fk_task >> finish_task