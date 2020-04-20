import ftplib
import time
import pika
import os

rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(os.environ.get('RABBITMQ_HOST')))
channel = rabbit_conn.channel()
channel.queue_declare(queue='ftp_paths')


def connector():
    ftp_connection = ftplib.FTP("ftp.ebi.ac.uk")
    ftp_connection.login()

    #ftp_connection.set_pasv(True)
    return ftp_connection


def parser():
    conn = connector()
    conn.cwd("/pub/databases/pmc/suppl/OA")
    dirs = conn.nlst()
    conn.close()

    for d in dirs:
        time.sleep(5)
        new_conn = ftplib.FTP("ftp.ebi.ac.uk")
        new_conn.login()
        new_conn_location = "/pub/databases/pmc/suppl/OA/%s" % d
        new_conn.cwd(new_conn_location)
        print("new ftp connection now")
        new_dir = new_conn.pwd()
        files_list = new_conn.nlst()
        for files in files_list:
            new_path = new_dir + '/' + files
            channel.basic_publish(exchange='', routing_key='ftp_paths', body={'path': new_path})
            if files == files_list[-1]:
                new_conn.close()
                break
            else:
                continue

if __name__ == '__main__':
    parser()
