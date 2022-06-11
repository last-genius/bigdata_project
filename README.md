Launch Kafka and Spark

```
./run-kafka.sh
docker-compose up -d
```

Launch the writer (it will write the stream data into a Kafka topic):

```
./kafka_write.sh
./kafka_read.sh
./run-server.sh
```

You can launch the requests from the `requests.http` file.

Here are the example requests:

B-1: Return the list of existing domains for which pages were created.

![](./img/1.png)

B-2: Return all the pages which were created by the user with a specified user_id.

![](./img/2.png)

B-3: Return the number of articles created for a specified domain.

![](./img/3.png)

B-4: Return the page with the specified page_id

![](./img/4.png)

B-5: Return the id, name, and the number of created pages of all the users who created
at least one page in a specified time range.

![](./img/5.png)


To shut it all down:

```
docker-compose down
./shutdown-cluster.sh
```