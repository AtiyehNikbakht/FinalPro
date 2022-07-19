from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import time

cluster = Cluster()
session = cluster.connect('finalkey')
consumer = KafkaConsumer('cassandratopic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)

l = []
a = 0
if __name__ == "__main__":
    for message in consumer:
        a = message.value.decode('utf-8')
        l = a.split(',')
        l[1] = float(l[1])
        l[2] = float(l[2])
        l[6] = int(l[6])
        l[7] = int(l[7])
        l[9] = int(l[9])
        l[12] = int(l[12])
        session.execute("INSERT INTO latlontbl (datetime, lat, lon, Base, datetimefloor, date, hour, weekdaynumber, weekdayname, weekmonthnumber, weekmonthname, geo, weeknumber, time, time12, id) VALUES ('%s', %f, %f, '%s', '%s', '%s', %d, %d, '%s',%d, '%s','%s',%d,'%s','%s', uuid())" % (l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11], l[12], l[13], l[14]))
        print("done!")
        time.sleep(5)










