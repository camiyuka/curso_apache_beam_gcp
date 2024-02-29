#pip install google-cloud-pubsub
#producer
# streaming 1

import csv
import time
from google.cloud import pubsub_v1
import os

service_account_key = r"C:\\Users\\camila yatabe\Downloads\dataflow-apache-beam-415812-f900aff91ba8.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

topico = 'projects/dataflow-apache-beam-415812/topics/meus_voos'
publisher = pubsub_v1.PublisherClient() # método de publisher

entrada = r"C:\\Users\\camila yatabe\Downloads\\voos_sample.csv"

with open(entrada, 'rb') as file:
    for row in file:
        print('publicando no tópico :)')
        publisher.publish(topico,row)
        time.sleep(2)