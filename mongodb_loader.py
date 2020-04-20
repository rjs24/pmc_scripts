import pika
from pymongo import MongoClient
import os

conn = pika.BlockingConnection(
    pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST')))
ch = conn.channel()
mongo_client_str = "mongodb://%s:%s@ip_address:27017/admin" % (os.environ.get('MONGODB_USERNAME'), os.environ.get('MONGODB_PASSWORD'))
mongo_client = MongoClient(mongo_client_str)
db = mongo_client['local']
collection = db['pmcs']
ch.queue_declare(queue='db_done')


def callback(ch, method, properties, body):
    print("Received: %s" % body)
    db_id = collection.insert_one(body.path)
    ch.basic_publish(exchange='', routing_key='db_done', body={'DB_ObjectId': db_id})


ch.basic_consume(
    queue='ftp_paths', on_message_callback=callback, auto_ack=True)
