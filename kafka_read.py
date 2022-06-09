from json import loads
from kafka import KafkaConsumer

consumer = KafkaConsumer('changes',
                         bootstrap_servers='kafka-server:9092',
                         value_deserializer=lambda x: loads(x.decode('ascii')))

for change in consumer:
    print(change)
