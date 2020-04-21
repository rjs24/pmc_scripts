import pika
from pymongo import MongoClient
import os
from bson.objectid import ObjectId

paths_list = []
def callback(ch, method, properties, body):
    print("Received: %s" % body)
    mong_data = body.decode("utf-8")
    print(mong_data)
    record = collection.find({"_id": ObjectId(mong_data) })
    pmc = [r for r in record]
    print(pmc)
    if pmc != []:
        for pths in paths_list:
            print(pths + " : " + pmc[0]['pmc'])
            if pmc[0]['pmc'] == pths.split('/')[-1].replace(".xml",".zip").replace(".txt",".zip") and pths.split('.')[-1] == 'xml':
                collection.update({ "_id": ObjectId(mong_data) }, {"xml_path": pths})
            elif pmc[0]['pmc'] == pths.split('/')[-1].replace(".xml",".zip").replace(".txt",".zip") and pths.split('.')[-1] == 'txt':
                collection.update({ "_id": ObjectId(mong_data) }, {"txt_path": pths})
            else:
                print("no path match")
                ch.basic_publish(exchange='', routing_key='db_no_match', body=mong_data)
                continue
    else:
        print("no db match")
        ch.basic_publish(exchange='', routing_key='db_no_match', body=mong_data)



if __name__ == "__main__":

    for dirs, subdirs, files in os.walk(".."):
        for f in files:
            file_path = os.path.join(dirs, f)
            full_path = os.environ.get("HOME") + file_path.replace("..","")
            print(full_path)
            paths_list.append(full_path)

    conn = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST')))
    ch = conn.channel()
    mongo_client_str = "mongodb://%s:%s@127.0.0.1:27017/admin" % (os.environ.get('MONGODB_USERNAME'), os.environ.get('MONGODB_PASSWORD'))
    mongo_client = MongoClient(mongo_client_str)
    db = mongo_client['local']
    ch.queue_declare(queue='db_no_match')
    collection = db['pmcs']
    ch.basic_consume(
        queue='db_done', on_message_callback=callback, auto_ack=True)
    ch.start_consuming()

