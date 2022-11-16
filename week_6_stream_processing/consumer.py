from kafka import KafkaConsumer
from json import loads
from time import sleep
import kafka

consumer = KafkaConsumer(
    'g1','g2','g3',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='consumer.group.id.demo.2',
     value_deserializer=lambda x: loads(x.decode('utf-8')))


while(True):
 print("inside while")
 for message in consumer:
    # if message.partition == 5:
    print(message.topic,message.value,message.partition)
  

 sleep(1)


