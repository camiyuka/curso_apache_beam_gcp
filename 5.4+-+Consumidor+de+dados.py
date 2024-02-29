# consumer 
# streaming 1


import csv
import time
from google.cloud import pubsub_v1
import os


service_account_key = r"C:\\Users\\camila yatabe\Downloads\dataflow-apache-beam-415812-f900aff91ba8.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

subscription = 'projects/dataflow-apache-beam-415812/subscriptions/meus_voos-sub'
subscriber = pubsub_v1.SubscriberClient() # método de subscriber

def monstrar_msg(mensagem):
  print(('Mensagem: {}'.format(mensagem)))
  mensagem.ack() # método acknowlodge- vai avisar o pubsub que a msg foi reconhecida

subscriber.subscribe(subscription,callback=monstrar_msg)

while True:
  time.sleep(3)