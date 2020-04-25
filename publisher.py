import time
import pika
import os
import random
import time
import requests
from bs4 import BeautifulSoup

rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(os.environ.get('RABBITMQ_HOST')))
channel = rabbit_conn.channel()
channel.queue_declare(queue='ftp_paths')


def file_opener():
    with open("dirslist.txt", "r") as file:
        dirs_list = file.readlines()
        return dirs_list

def parser():
    dirs = file_opener()
    print(dirs)
    max_index = len(dirs)
    done_list = []
    counter = 0
    while len(done_list) != max_index:
        counter += 1
        current_list = [x for x in dirs if x not in done_list]
        print(len(done_list), len(current_list))
        current_target = random.choice(current_list)
        print(current_target)
        done_list.append(current_target)
        try:
            new_conn_location = "http://ftp.ebi.ac.uk/pub/databases/pmc/suppl/NON-OA/%s" % current_target.replace("\n", "")
            req = requests.get(new_conn_location)
            print("request status:  ", req.status_code)
            if req.status_code == 200:
                soup = BeautifulSoup(req.text)
                print("soup length:  ", len(soup.findAll('a')))
                if len(soup.findAll('a')) <= 1:
                    print("current target: %s has no files inside" % current_target)
                    continue
                else:
                    for files in soup.findAll('a'):
                        print("files: ", files['href'])
                        if "PMC" in files['href']:
                            new_path = "/pub/databases/pmc/suppl/NON-OA/%s" % current_target.replace("\n","") + files['href']
                            channel.basic_publish(exchange='', routing_key='ftp_paths', body=new_path)
                            """
                            if files == list(soup.findAll('a'))[-1]:
                                if counter % 5 == 0:
                                    random_sleep = random.randint(300, 900)
                                    time.sleep(random_sleep)
                                    break
                                else:
                                    break
                            else:
                                continue
                            """
                        else:
                            continue
            else:
                print(req.status_code)
                time.sleep(3600)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    parser()
