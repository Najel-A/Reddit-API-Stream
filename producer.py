from kafka import KafkaProducer
import praw
import re
import json
import os
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

load_dotenv()

# Initialize VADER Sentiment Analyzer
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Reddit API credentials
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
USER_AGENT = os.getenv("USER_AGENT")

# Initialize Reddit API
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=USER_AGENT
)

# Define subreddit and keyword
SUBREDDIT_NAME = "all"  # Change as needed
KEYWORD = "AI"

# Function to clean text
def clean_text(text):
    text = re.sub(r"http\S+", "", text)  # Remove URLs
    text = re.sub(r"[^\w\s]", "", text)  # Remove special characters
    return text.strip()

# Function to analyze sentiment
def analyze_sentiment(text):
    scores = sia.polarity_scores(text)
    if scores["compound"] >= 0.05:
        return "Positive"
    elif scores["compound"] <= -0.05:
        return "Negative"
    else:
        return "Neutral"

# Stream Reddit posts and send to Kafka
subreddit = reddit.subreddit(SUBREDDIT_NAME)

for submission in subreddit.stream.submissions(skip_existing=True):
    if KEYWORD.lower() in submission.title.lower() or KEYWORD.lower() in submission.selftext.lower():
        cleaned_text = clean_text(submission.selftext)
        sentiment = analyze_sentiment(cleaned_text)

        message = {
            "title": submission.title,
            "text": cleaned_text,
            "sentiment": sentiment,
            "url": submission.url
        }

        producer.send("reddit_sentiment", message)
        print(f"Sent to Kafka: {message}")
