#!/bin/bash

docker run --rm -it --network project-network --name spark-submit \
    -v /home/lastgenius/Documents/bigdata_project:/opt/app bitnami/spark:3 /bin/bash \
    -c "cd /opt/app && spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 --deploy-mode client project.py"
    #-c "cd /opt/app && spark-submit --conf spark.jars.ivy=/opt/app --packages \"org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0\" --master spark://experiments-spark-1:7077 --deploy-mode client project.py"