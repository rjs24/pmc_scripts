import ftplib
import time
import pika
import os
import random
import time

rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(os.environ.get('RABBITMQ_HOST')))
channel = rabbit_conn.channel()
channel.queue_declare(queue='ftp_paths')


def connector():
    ftp_connection = ftplib.FTP("ftp.ebi.ac.uk")
    ftp_connection.login()

    #ftp_connection.set_pasv(True)
    return ftp_connection

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
        time.sleep(3)
        try:
            new_conn = connector()
            new_conn_location = "/pub/databases/pmc/suppl/NON-OA/%s" % current_target.replace("\n", "")
            new_conn.cwd(new_conn_location)
            print("new ftp connection now", new_conn)
            current_path = new_conn.pwd()
            files_list = new_conn.nlst()
            if files_list == []:
                print("current target: %s has no files inside" % current_target)
                continue
            else:
                for files in files_list:
                    new_path = current_path + '/' + files
                    channel.basic_publish(exchange='', routing_key='ftp_paths', body=new_path)
                    if files == files_list[-1]:
                        new_conn.close()
                        del new_conn
                        if counter % 5 == 0:
                            random_sleep = random.randint(300, 900)
                            time.sleep(random_sleep)
                            break
                        else:
                            break
        except Exception as e:
            print(e)


if __name__ == '__main__':
    parser()
