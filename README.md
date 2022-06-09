Launch Kafka and Spark

```
./run-kafka.sh
docker-compose up -d
```

Launch the writer (it will write the stream data into a Kafka topic):

```
./kafka_write.sh
./kafka_read.py
```
