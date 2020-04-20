import pika
from pymongo import MongoClient
import os


def callback(ch, method, properties, body):
    print("Received: %s" % body)
    mong_data = body.decode("utf-8")
    record = collection.find({"_id": mong_data})
    



if __name__ == "__main__":
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST')))
    ch = conn.channel()
    mongo_client_str = "mongodb://%s:%s@127.0.0.1:27017/admin" % (os.environ.get('MONGODB_USERNAME'), os.environ.get('MONGODB_PASSWORD'))
    mongo_client = MongoClient(mongo_client_str)
    db = mongo_client['local']
    collection = db['pmcs']
    ch.basic_consume(
        queue='db_done', on_message_callback=callback, auto_ack=True)
    ch.start_consuming()

