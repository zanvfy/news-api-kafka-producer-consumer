import json
import csv

from kafka import KafkaConsumer
import csv

file_path = '/home/contentwrito/assignment/midterm/article_data.csv'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'news-article-topics',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Define the CSV fieldnames based on the structure of the article data
fieldnames = ['title', 'description', 'url', 'publishedAt', 'content']

with open(file_path, 'w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()

    for message in consumer:
        article_data = message.value

        # Write each article to the CSV file
        writer.writerow({
            'title': article_data['title'],
            'description': article_data['description'] if 'description' in article_data else '',
            'url': article_data['url'] if 'url' in article_data else '',
            'publishedAt': article_data['publishedAt'] if 'publishedAt' in article_data else '',
            'content': article_data['content'] if 'content' in article_data else ''
        })
        print(f"Article '{article_data['title']}' has been written to the CSV file")
