from time import sleep
from json import dumps
from kafka import KafkaProducer
import kafka
import pandas as pd
import numpy as np








def producer():
    toparr=['g1','g2','g3']
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
    df=pd.read_csv('D:/data-engineering-zoomcamp-main/week_6_stream_processing/circuit.csv',delimiter=",")
    
    df['name'] = df['name'].replace({np.NaN:'NOOOOO'})
    
    #form=df.fillna("No name") 
    form = df
    

    



    
    
    newdata1=form.iloc[0:25].loc[df['country'] == "USA"]
    newdata2=form.iloc[25:50].loc[df['country'] == "Pak"]
    newdata3=form.iloc[50:].loc[df['country'] == "Ind"]
    
    for topic in toparr:
        if topic == 'g1':
            for row in newdata1.itertuples():
                producer.send(topic=topic, value=row)
                producer.flush()
                print("producing")
                sleep(1)
        if topic == 'g2':
            for row in newdata2.itertuples():
                producer.send(topic=topic, value=row)
                producer.flush()
                print("producing")
                sleep(1)
        if topic == 'g3':
            for row in newdata3.itertuples():
                producer.send(topic=topic, value=row)
                producer.flush()
                print("producing")
                sleep(1)

       





if __name__ == '__main__':
    producer()




# def producer2():
#     producer2 = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                          value_serializer=lambda x:
#                          dumps(x).encode('utf-8'))
#     #with open('D:/data-engineering-zoomcamp-main/week_6_stream_processing/circuit.csv') as file:
#     df=pd.read_csv('D:/data-engineering-zoomcamp-main/week_6_stream_processing/circuit.csv',delimiter=",")
#     form=df.fillna("No name")
#     for row in form.itertuples():
#         producer2.send(topic='a2', value=row)
#         producer2.flush()
#         print("producing")
#         sleep(1)

       





# if __name__ == '__main__':
#     producer2()



# def producer3():
#     producer3 = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                          value_serializer=lambda x:
#                          dumps(x).encode('utf-8'))
#     #with open('D:/data-engineering-zoomcamp-main/week_6_stream_processing/circuit.csv') as file:
#     df=pd.read_csv('D:/data-engineering-zoomcamp-main/week_6_stream_processing/circuit.csv',delimiter=",")
#     form=df.fillna("No name")
#     for row in form.itertuples():
#         producer3.send(topic='a3', value=row)
#         producer3.flush()
#         print("producing")
#         sleep(1)

       





# if __name__ == '__main__':
#     producer3()



    




    