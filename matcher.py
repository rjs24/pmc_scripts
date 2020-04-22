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
    local_pmcs_list = [p.split('/')[-1] for p in paths_list]
    if pmc != []:
        if pmc[0]['pmc'].replace(".zip",".txt") in local_pmcs_list:
            index = local_pmcs_list.index(pmc[0]['pmc'].replace(".zip",".txt"))
            collection.update({ "_id": ObjectId(mong_data) }, {"xml_path": paths_list[index]})
        elif pmc[0]['pmc'].replace(".zip",".xml") in local_pmcs_list:
            index = local_pmcs_list.index(pmc[0]['pmc'].replace(".zip",".xml"))
            collection.update({ "_id": ObjectId(mong_data) }, {"txt_path": paths_list[index]})
    else:
        print("no path match")



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
    collection = db['pmcs']
    ch.basic_consume(
        queue='db_done', on_message_callback=callback, auto_ack=True)
    ch.start_consuming()

