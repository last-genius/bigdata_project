from curses.panel import top_panel
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from datetime import datetime, timedelta


class CassandraClient:
    def __init__(self):
        self.session = None
        self.schema = "date timestamp, domain string, page_id long, page_title string, url string, " \
            "user_id long, user_is_bot boolean, user_text string, interval int"

    def create(self):
        self.spark = SparkSession \
            .builder \
            .appName("Spark Kafka Streaming") \
            .getOrCreate()
        self.df = self.spark.createDataFrame([], schema=self.schema)

    def connect(self):
        cluster = Cluster(['cassandra-server'], port=9042)
        self.session = cluster.connect('project')

    def write(self, data):
        dt = datetime.strptime(data["meta"]["dt"].replace(
            "T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
        data_dict = [{
            "domain": str(data["meta"]["domain"]),
            "url": data["meta"]["uri"],
            "user_id": data["performer"]["user_id"] if "user_id" in data["performer"] else 0,
            "user_text": data["performer"]["user_text"],
            "date": dt,
            "user_is_bot": data["performer"]["user_is_bot"],
            "page_title": data["page_title"],
            "page_id": data["page_id"],
            "interval": dt.hour,
        }]
        # print("interval", dt.hour)

        append_table = self.spark.createDataFrame(
            data_dict, schema=self.schema)
        # append_table.printSchema()
        self.df = self.df.union(append_table)
        data_dict = data_dict[0]

        self.session.execute(f"INSERT INTO created_domains (domain, url) "
                             f"VALUES ('{data_dict['domain']}', '{data_dict['url']}')")
        self.session.execute(f"INSERT INTO pages_by_userid (user_id, user_text, url, date) "
                             f"VALUES ({data_dict['user_id']}, '{data_dict['user_text']}', '{data_dict['url']}', '{data_dict['date']}')")
        self.session.execute(f"INSERT INTO pageids (page_id, url) "
                             f"VALUES ({data_dict['page_id']}, '{data_dict['url']}')")

    def update(self):
        self.df = self.df.filter(
            self.df["interval"] != (datetime.utcnow() - timedelta(hours=6)).hour)

        # print("our hour", (datetime.utcnow() - timedelta(hours=1)).hour)
        last_hour_data = self.df.filter(self.df["interval"] == (
            datetime.utcnow() - timedelta(hours=0)).hour)
        last_hour_data = last_hour_data.groupBy(
            self.df["domain"]).count().collect()
        # print(last_hour_data)
        statistics = []
        for row in last_hour_data:
            statistics.append((row["domain"], row["count"]))
        self.session.execute(f"INSERT INTO category_a_1 (time_start, time_end, statistics) "
                             f"VALUES ({(datetime.utcnow() - timedelta(hours=2)).hour}, "
                             f"{(datetime.utcnow() - timedelta(hours=1)).hour}, "
                             "{" + ','.join([f"'{key}': {val}" for key, val in statistics]) + "})")

        start_date = (datetime.utcnow() - timedelta(hours=7)).hour
        end_date = (datetime.utcnow() - timedelta(hours=1)).hour
        bots_rows = self.df.filter(self.df["user_is_bot"] == "True") \
            .groupBy(self.df["domain"]).count().collect()
        statistics = []
        for row in bots_rows:
            statistics.append({'domain': row['domain'],
                               'created_by_bots': row['count']})
        self.session.execute(f"INSERT INTO category_a_2 (time_start, time_end, statistics) "
                             f"VALUES ({start_date}, {end_date}, "
                             f"{str(statistics)})")

        statistics = []
        top_20_users = self.df.groupBy(self.df["user_text"]).count() \
            .sort(desc("count")).limit(20).collect()
        for row in top_20_users:
            user_pages = self.df.filter(self.df["user_text"] == row["user_text"]).collect()
            user_row = {"user_text": user_pages[0]["user_text"],
                        "user_id": user_pages[0]["user_id"],
                        "count": len(user_pages),
                        "titles": [i.page_title for i in user_pages]}
            
            statistics.append(user_row)

        # print(str(statistics))
        self.session.execute(f"INSERT INTO category_a_3 (time_start, time_end, statistics) "
                            f"VALUES ({start_date}, {end_date}, " \
                            f"{str(statistics)})")

    def select_a1(self, data):
        last_hour = (datetime.utcnow() - timedelta(hours=6)).hour
        return list(self.session.execute(f"SELECT * from category_a_1 WHERE time_start > {last_hour} ALLOW FILTERING"))

    def select_a2(self, data):
        last_hour = (datetime.utcnow() - timedelta(hours=6)).hour
        return list(self.session.execute(f"SELECT * from category_a_2 WHERE time_start > {last_hour} ALLOW FILTERING"))

    def select_a3(self, data):
        last_hour = (datetime.utcnow() - timedelta(hours=6)).hour
        return list(self.session.execute(f"SELECT * from category_a_3 WHERE time_start > {last_hour} ALLOW FILTERING"))

    def select_created_domains(self, data):
        return list(self.session.execute("SELECT DISTINCT domain from created_domains"))

    def select_user_created_pages(self, data):
        return list(self.session.execute(f"SELECT url from pages_by_userid WHERE user_id={data['user_id']}"))

    def select_created_articles(self, data):
        return list(self.session.execute(f"SELECT COUNT(*) from created_domains WHERE domain='{data['domain']}'"))

    def select_pageid(self, data):
        return list(self.session.execute(f"SELECT url FROM pageids WHERE page_id={data['page_id']}"))

    def select_all_created_pages(self, data):
        start_date = datetime.strptime(data["start_date"], "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(data["end_date"], "%Y-%m-%d %H:%M:%S")
        return list(self.session.execute(f"SELECT user_id, user_text, COUNT(*) FROM pages_by_userid "
                                         f"WHERE date >= '{start_date}' AND date <= '{end_date}' ALLOW FILTERING"))
