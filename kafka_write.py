import json
from kafka import KafkaProducer
from sseclient import SSEClient as EventSource

producer = KafkaProducer(bootstrap_servers='kafka-server:9092',
                         value_serializer=lambda x: json.dumps(x).encode('ascii'))
url = 'https://stream.wikimedia.org/v2/stream/page-create'

for event in EventSource(url):
    if event.event == 'message':
        try:
            producer.send('changes', json.loads(event.data))
        except ValueError:
            pass

    producer.flush()

