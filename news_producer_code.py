import time
import json
import re
from kafka import KafkaProducer
from newsapi import NewsApiClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize News API client
newsapi = NewsApiClient(api_key='7d67c637cefd44e9bb8e8609604e9874')

# Define the list of media sources
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today'

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')


def clean_text(text):
    # Remove special characters and excess whitespaces
    cleaned_text = re.sub(r'\s+', ' ', text)  # Remove excess whitespaces
    cleaned_text = re.sub(r'[^\w\s]', '', cleaned_text)  # Remove special characters
    return cleaned_text.strip()


try:
    while True:
        # Fetch articles from NewsAPI
        all_articles = newsapi.get_everything(q='jobs', sources=sources, language='en', page=4)

        # Preprocess and publish articles to Kafka
        for article in all_articles['articles']:
            # Clean up article text
            cleaned_article = {
                'title': clean_text(article['title']),
                'description': clean_text(article['description']) if 'description' in article else '',
                'url': clean_text(article['url']),
                'publishedAt': clean_text(article['publishedAt']),
                'content': clean_text(article['content']) if 'content' in article else ''
            }

            logging.info(f"Publishing article: {cleaned_article['title']}")
            producer.send('news-article-topics', json.dumps(cleaned_article).encode('utf-8'))

        # Wait for 1 minute before fetching articles again
        time.sleep(60)  # 60 seconds

except Exception as e:
    logging.error(f"An error occurred: {str(e)}")
finally:
    producer.close()