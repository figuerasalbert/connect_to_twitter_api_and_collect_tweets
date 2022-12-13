"""
This python module will obtain tweets from the Twitter account @PremierInjuries.
Specifically, it will collect tweets that include in their content the hashtag
'#FPL', as these are the tweets used by the account to report injured, suspended or
recovered players. The frequency of the collection of tweets will be from week to week.

"""

# Import the required packages
import pandas as pd
import psycopg2
import tweepy
import configparser
import datetime
from functions.execute_values import execute_values



#---------------------- Twitter API Credentials ----------------------
# Read configs
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']

access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

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

print(str(len(twitter_df.index)) + ' Tweets successfully obtained')


#---------------------- Data Lake Credentials ----------------------
# Read configs
config = configparser.ConfigParser()
config.read('config.ini')

host = config['data_lake']['host']
database = config['data_lake']['database']

user = config['data_lake']['user']
password = config['data_lake']['password']


# ----------------------------- Connect to Data Lake -----------------------------
conn = psycopg2.connect(host = host,
                        database = database,
                        user = user,
                        password = password)
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

# Auto commit is very important
conn.set_session(autocommit=True)


# ----------------------------- Drop PremierInjuries_Twitter Table -----------------------------
try:
    cur.execute("DROP TABLE PremierInjuries_Twitter_weekly;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')


# ----------------------------- Create new PremierInjuries_Twitter Table -----------------------------
try:
    cur.execute("CREATE TABLE IF NOT EXISTS PremierInjuries_Twitter_weekly (Twitter_User VARCHAR(255), Tweet VARCHAR(512),\
                Tweet_Date VARCHAR(255))")
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')


#----------------------------- Full Load for PremierInjuries_Twitter Table -----------------------------
execute_values(conn, twitter_df, 'PremierInjuries_Twitter_weekly')

cur.close()
conn.close()