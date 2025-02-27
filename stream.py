import praw
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from time import sleep
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize SparkSession
spark = SparkSession.builder.appName("RedditStreaming").getOrCreate()

# Reddit API credentials (hidden for security in .env file)
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")

# Connect to Reddit API
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=USER_AGENT
)

# Use keywords to filter posts
keyword = "AITA"
data = [] # Stores posts

# Preprocess text 
def clean_text(text):
    return re.sub(r"http[s]?://\S+", "", text)


print(f"Listening for new posts containing '{keyword}'...")

for submission in reddit.subreddit("all").stream.submissions():
    if keyword.lower() in submission.title.lower() or keyword.lower() in submission.selftext.lower():
        post = {"title": submission.title, "body": submission.selftext}
        post["clean_body"] = clean_text(post["body"])
        data.append(post)

        # Create Spark DataFrame
        df = spark.createDataFrame(data)

        # Process: Remove URLs
        df = df.withColumn("clean_body", regexp_replace(col("body"), "http[s]?://\S+", ""))

        # Show results
        df.select("title", "clean_body").show(truncate=False)

        # Sleep to avoid API rate limits
        sleep(2)