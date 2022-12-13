# ----------------------------- Import Required Packages -----------------------------
# Packages for DAG
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Packages for Twitter API
import pandas as pd
import tweepy
#import configparser
import datetime
from dotenv import load_dotenv
import os

# Packages for Connect to the Data Lake
from airflow.hooks.postgres_hook import PostgresHook


# ----------------------------- Define Functions -----------------------------
# 1. Log the start of the DAG
def start_dag():
    logging.info('Starting the DAG. Obtaining tweets from @PremierInjuries')

# 2. Connect to Twitter API and get the tweets
def run_twitter_api():
    #---------------------- Twitter API Credentials ----------------------
    # Read configs
    #config = configparser.ConfigParser()
    #config.read('my_config.ini')
    #config.sections()

    #api_key = config['twitter']['api_key']
    #api_key_secret = config['twitter']['api_key_secret']

    #access_token = config['twitter']['access_token']
    #access_token_secret = config['twitter']['access_token_secret']
    #api_key = "HwElQ5vIabPZFJd26PbFrvRnU"
    #api_key_secret = "l9WNDhziEzzVROMWlAWsNKUg57P3ZTD9B8SDaZJtil6G7kvpTC"
    #access_token = "1523632496998559746-yz0Z9Zc6Ja9pk9ZGjcNB27dulCAO29"
    #access_token_secret = "dOFz7UeCphlUdbscKL02YuDH26tmkJoMcM8E8gXU8k55e"

    #f = open("/dags/twitter_api_credentials.txt", "r")
    #lines = f.readlines()
    #api_key = (lines[0].split())[2]
    #api_key_secret = (lines[1].split())[2]
    #access_token = (lines[2].split())[2]
    #access_token_secret = (lines[3].split())[2]
    #f.close()

    load_dotenv()
    api_key = os.getenv('api_key')
    api_key_secret = os.getenv('api_key_secret')
    access_token = os.getenv('access_token')
    access_token_secret = os.getenv('access_token_secret')


    #---------------------- Create the connection with the Twitter API ----------------------
    # Authentification
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Create an API object
    api = tweepy.API(auth)

    #---------------------- Defining the criteria to get tweets ----------------------
    # Specifying the search query
    search_query = 'from:PremierInjuries #FPL'
    # Specifying the date since of the tweets
    date_since = datetime.date.today() - datetime.timedelta(days=7)
    # Specifying the number of tweets we want to extract in one run
    limit = 3250

    # Getting the tweets through the API
    tweets = tweepy.Cursor(api.search_tweets,
                        search_query, since_id = date_since,
                        tweet_mode = 'extended').items(limit)

    #---------------------- Save the tweets in a DataFrame ----------------------
    # Create DataFrame
    columns = ['Twitter_User', 'Tweet', 'Tweet_Date']
    data_tweets = []

    for tweet in tweets:
        data_tweets.append([tweet.user.screen_name, tweet.full_text, tweet.created_at])

    twitter_df = pd.DataFrame(data_tweets, columns = columns)

    # Changing the format of column Date, eliminating the time zone
    twitter_df['Tweet_Date'] = pd.to_datetime(twitter_df.Tweet_Date).dt.tz_localize(None)
    twitter_df['Tweet_Date'] = twitter_df['Tweet_Date'].dt.strftime('%Y-%m-%d')
    print(str(len(twitter_df.index)) + ' Tweets successfully obtained')

    tweets_list = [tuple(x) for x in twitter_df.to_numpy()]

    return tweets_list

# 3. Load Tweets to the Data Lake
def load_data_lake(ti):
    # Get the data from the previous task
    data = ti.xcom_pull(task_ids = ['get_tweets_task'])
    if not data:
        raise ValueError('No value currently stored in XComs')

    # Extract twitter data from nested list
    twitter_data = data[0]

    # Data Lake Credentials
    pg_hook = PostgresHook(
        postgres_conn_id = 'twitter_api_airflow',
        schema = 'twitter_api'
    )

    # Drop PremierInjuries_Twitter Table
    sql_drop_table = "DROP TABLE PremierInjuries_Twitter_weekly;"

    # Create new PremierInjuries_Twitter Table
    sql_create_table = "CREATE TABLE IF NOT EXISTS PremierInjuries_Twitter_weekly (Twitter_User VARCHAR(255), Tweet VARCHAR(512), Tweet_Date VARCHAR(255), upload_time timestamp DEFAULT CURRENT_TIMESTAMP);"

    # Connect to Data Lake
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    # Execute SQL statements
    cursor.execute(sql_drop_table)
    cursor.execute(sql_create_table)

    # Commit
    pg_conn.commit()

    # Insert data into Data Lake
    pg_hook.insert_rows(table="PremierInjuries_Twitter_weekly", rows=twitter_data)

# 4. Log the end of the DAG
def finish_dag():
    logging.info('Ending the DAG. Obtained tweets from @PremierInjuries')


# ----------------------------- Create DAG -----------------------------
default_args = {
    'owner': 'albert',
    'start_date': datetime.datetime(2022, 12, 10)
}

dag = DAG('twitter_api_DAG',
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

# 2. API Connection and Get Tweets
get_tweets_task = PythonOperator(
    task_id = "get_tweets_task",
    python_callable = run_twitter_api,
    do_xcom_push = True,
    dag = dag
)

# 3. Load to Data Lake
load_data_lake_task = PythonOperator(
    task_id = "load_data_lake_task",
    python_callable = load_data_lake,
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
start_task >> get_tweets_task >> load_data_lake_task >> finish_task