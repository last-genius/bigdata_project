from datetime import datetime
from time import sleep
from json import loads
from cassandra_client import CassandraClient
from kafka import KafkaConsumer


while True:
    consumer = KafkaConsumer('changes',
                             bootstrap_servers='kafka-server:9092',
                             value_deserializer=lambda x: loads(x.decode('ascii')))

    client = CassandraClient()
    client.create()
    client.connect()

    last_hour = None
    added = 0
    for msg in consumer:
        if added % 100 == 0:
            print(added)
            print("user_id", msg.value["performer"]["user_id"] if "user_id" in msg.value["performer"] else 0)
            print("page_id", msg.value["page_id"])
        client.write(msg.value)
        added += 1
        
        if last_hour != datetime.strftime(datetime.utcnow(), "%H"):
            last_hour = datetime.strftime(datetime.utcnow(), "%H")
            client.update()
            print("updated")
