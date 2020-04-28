import requests
from pymongo import MongoClient
import os
from lxml import etree
import datetime
import time
import pika

meta_api = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pmc&id=3539452&retmode=json&tool=my_tool&email=my_email@example.com"
full_article_api = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=4304705&tool=my_tool&email=my_email@example.com"

rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(os.environ.get('RABBITMQ_HOST')))
channel = rabbit_conn.channel()
channel.queue_declare(queue='api_done')

def xml_parser(xml_string, collection):
    nw_record = {}
    tree = etree.fromstring(xml_string)
    for branch in tree.iter():
        if branch.tag == "journal-title":
            nw_record['publication_title'] = branch.text
            continue
        elif branch.tag == "article-meta":
            for b in branch:
                if b.tag == "article-id":
                    if b.items()[0][1] == 'pmc':
                        nw_record['pmc'] = b.text
                        continue
                    elif b.items()[0][1] == 'pmid':
                        nw_record['pmid'] = b.text
                        continue
                    elif b.items()[0][1] == 'doi':
                        nw_record['doi'] = b.text
                        continue
                    else:
                        continue
                elif b.tag == "title-group":
                    for title in b:
                        if title.tag == "article-title":
                            nw_record['title'] = title.text
                            continue
                        else:
                            continue
                elif b.tag == 'contrib-group':
                    n = 0
                    nw_record['authors'] = []
                    for contribs in b:
                        if contribs.tag == "contrib" and contribs.items()[-1][1] == "author":
                            for auths in contribs:
                                if auths.tag == "name":
                                    n += 1
                                    author_object = {}
                                    for field in auths:
                                        if field.tag == "surname":
                                            author_object['surname'] = field.text
                                            continue
                                        elif field.tag == "given-names":
                                            author_object['first_name'] = field.text
                                            continue
                                elif auths.tag == "xref" and auths.items()[-1][-1]:
                                    author_object['id'] = auths.items()[-1][-1]
                                    continue
                                elif auths.tag == "email":
                                    author_object['email_domain'] = auths.text.split('@')[-1]
                                    nw_record['authors'].append(author_object)
                                    continue
                elif b.tag == "aff":
                    for insts in b:
                        for authors in nw_record['authors']:
                            if authors['id'] == b.items()[-1][-1]:
                                for inst in b:
                                    if inst.tag == "addr-line":
                                        authors['institution'] = inst.text
                                        continue
                                    else:
                                        continue
                            else:
                                continue
                elif b.tag == "volume":
                    nw_record['volume'] = b.text
                    continue
                elif b.tag == "fpage":
                    nw_record['first_page'] = b.text
                    continue
                elif b.tag == "lpage":
                    nw_record['last_page'] = b.text
                    continue
                elif b.tag == "history":
                    for date in b:
                        if date.tag == "date" and date.items()[-1][-1] == 'accepted':
                            for accepted in date:
                                if accepted.tag == "day":
                                    day = accepted.text
                                    continue
                                elif accepted.tag == "month":
                                    month = accepted.text
                                    continue
                                elif accepted.tag == "year":
                                    year = accepted.text
                                    continue
                            nw_record['publication_date'] = datetime.datetime(int(year), int(month), int(day))
                        else:
                            continue
                elif b.tag == "abstract":
                    nw_record['abstract'] = ''
                    for txt in b.itertext():
                        if txt != "\n":
                            nw_record['abstract'] += txt
                        else:
                            continue
                    continue
        elif branch.tag == "body":
            body = ''
            for b in branch.itertext():
                body += b
            file_string = "PMC" + nw_record['pmc'] + "_body.txt"
            with open(file_string, "w") as file:
                file.writelines(body)
            nw_record['body_filepath'] = os.path.realpath(file_string)
        else:
            continue
    pmc_query_string = "PMC" + nw_record['pmc'] + ".zip"
    collection.update_one({"pmc": pmc_query_string}, {"$set" : nw_record })

def db_connector():
    mongo_client_str = "mongodb://%s:%s@%s:27017/admin" % (
    os.environ.get('MONGODB_USERNAME'), os.environ.get('MONGODB_PASSWORD'), os.environ.get('MONGODB_HOST'))
    mongo_client = MongoClient(mongo_client_str)
    db = mongo_client['local']
    collection = db['pmcs']
    return collection

def get_api():
    collection = db_connector()

    while True:
        pmc = collection.find_one({'title': {'$exists': False }})
        pmc_string = pmc['pmc'].replace('PMC',"").replace(".zip","")
        full_article_api_string = \
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=%s&tool=my_tool&email=my_email@example.com" \
        % pmc_string
        req = requests.get(full_article_api_string, params={'tool': "text_miner", "email": "rjseacome@gmail.com"})
        time.sleep(3)
        if req.status_code == 200:
            xml = req.text
            xml_parser(xml, collection)
            message = str(pmc['_id'])
            channel.basic_publish(exchange='', routing_key='api_done', body=message)
        else:
            print("api failed")

if __name__ == "__main__":
    get_api()