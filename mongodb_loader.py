import pika
from pymongo import MongoClient
import os


def callback(ch, method, properties, body):
    print("Received: %s" % body)
    mong_data = body.decode("utf-8")
    pmc = mong_data.split('/')[-1]
    db_id = collection.insert_one({"path": mong_data, "pmc": pmc}).inserted_id
    ch.basic_publish(exchange='', routing_key='db_done', body=str(db_id))



if __name__ == "__main__":
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST')))
    ch = conn.channel()
    mongo_client_str = "mongodb://%s:%s@127.0.0.1:27017/admin" % (os.environ.get('MONGODB_USERNAME'), os.environ.get('MONGODB_PASSWORD'))
    mongo_client = MongoClient(mongo_client_str)
    db = mongo_client['local']
    collection = db['pmcs']
    ch.queue_declare(queue='db_done')
    ch.basic_consume(
        queue='ftp_paths', on_message_callback=callback, auto_ack=True)
    ch.start_consuming()

