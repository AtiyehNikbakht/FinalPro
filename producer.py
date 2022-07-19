from kafka import KafkaProducer
import time
import json
from csv import reader

producer = KafkaProducer(bootstrap_servers=['localhost:9092']) #, value_serializer=jsonConv)

if __name__ == '__main__':
    with open('myData.csv', 'r') as read_obj:
        csv_dict_reader = reader(read_obj)
        lineList = list(csv_dict_reader)
        del lineList[0]
    for i in lineList:
        data = ','.join(map(str, i))
        print(data)
        producer.send('testTwo', data.encode('utf-8'))
        time.sleep(5)




