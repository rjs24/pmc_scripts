from pymongo import MongoClient
import os
import pika
import aspell
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords
import json
from bson.objectid import ObjectId

rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(os.environ.get('RABBITMQ_HOST')))
channel = rabbit_conn.channel()


def db_connector():

    mongo_client_str = "mongodb://%s:%s@%s:27017/admin" % (
    os.environ.get('MONGODB_USERNAME'), os.environ.get('MONGODB_PASSWORD'), os.environ.get('MONGODB_HOST'))
    mongo_client = MongoClient(mongo_client_str)
    db = mongo_client['local']
    collection = db['pmcs']
    return collection


def spell_check(channel, method, properties, body):

    pmc_num = body.decode('utf-8').replace("PMC","").replace(".zip","")
    pmc_record = collection.find_one({'pmc': pmc_num})
    body_path = pmc_record['body_filepath']
    with open(body_path, "r") as file:
        contents = file.readlines()
    processed_list = word_tokenize(''.join(contents))
    stop_words = set(stopwords.words("english"))
    common_removed_list = [x for x in processed_list if x not in stop_words]
    speller = aspell.Speller('lang', 'en')
    spellchecked_list = []
    for words in common_removed_list:
        check = speller.check(words)
        if check == True or words.isdigit() or len(words) <= 2:
            continue
        elif check == False:
            if not any(s.isalpha() for s in words):
                continue
            else:
                spellchecked_list.append(words.strip('/').strip("'"))

    spellchecked_frequency = FreqDist(spellchecked_list)
    locations_list = []
    for n, terms in enumerate(processed_list):
        if terms in spellchecked_list:
            locations_list.append((terms, n))
            continue
        else:
            continue
    locations_file_str = body_path.replace(".txt","") +"_locations.txt"
    with open(locations_file_str, "w") as locations_file:
        locations_file.writelines(json.dumps(locations_list))
    collection.update_one({'pmc': pmc_num},{'$set':{'word_frequency': spellchecked_frequency, 'locations_path': locations_file_str }})
    channel.basic_publish(exchange='', routing_key='spelling_done', body=pmc_num)


if __name__ == "__main__":

    collection = db_connector()
    channel.queue_declare(queue='spelling_done')
    channel.basic_consume(
        queue='api_done', on_message_callback=spell_check, auto_ack=True)
    channel.start_consuming()