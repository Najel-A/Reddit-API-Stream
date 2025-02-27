from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import praw
import re
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize VADER Sentiment Analyzer
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()

# Initialize Spark Session
spark = SparkSession.builder.appName("RedditSentimentAnalysis").getOrCreate()

# Reddit API credentials (hidden for security in .env file)
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")

# Initialize Reddit API connection
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=USER_AGENT
)

# Define the keyword to track
KEYWORD = "tech"

# UDF (User Defined Function) to clean text (removing URLs and special characters)
def clean_text(text):
    text = re.sub(r"http\S+", "", text)  # Remove URLs
    text = re.sub(r"[^\w\s]", "", text)  # Remove special characters
    return text

# UDF to analyze sentiment
def analyze_sentiment(text):
    scores = sia.polarity_scores(text)
    if scores["compound"] >= 0.05:
        return "Positive"
    elif scores["compound"] <= -0.05:
        return "Negative"
    else:
        return "Neutral"

# Convert UDFs to PySpark functions
clean_text_udf = udf(clean_text, StringType())
analyze_sentiment_udf = udf(analyze_sentiment, StringType())

# Collect Reddit posts
data = []
subreddit = reddit.subreddit("all")  # Change to subbredit if necessary

for submission in subreddit.stream.submissions(skip_existing=True):
    if KEYWORD.lower() in submission.title.lower() or KEYWORD.lower() in submission.selftext.lower():
        cleaned_text = clean_text(submission.selftext)
        sentiment = analyze_sentiment(cleaned_text)
        
        data.append((submission.title, cleaned_text, sentiment, submission.url))
        
        # Process data in PySpark once we collect a batch
        if len(data) >= 5:
            df = spark.createDataFrame(data, ["Title", "Text", "Sentiment", "URL"])
            df = df.withColumn("Cleaned_Text", clean_text_udf(df["Text"]))
            df = df.withColumn("Sentiment", analyze_sentiment_udf(df["Cleaned_Text"]))
            
            print("\nProcessed Sentiment Analysis:")
            df.show(truncate=False)
            
            data = []  # Reset the batch
