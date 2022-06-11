CREATE KEYSPACE project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE project;

DROP TABLE IF EXISTS created_domains;
DROP TABLE IF EXISTS pages_by_userid;
DROP TABLE IF EXISTS pageids;

CREATE TABLE created_domains 
(
    domain text,
    url text,
    PRIMARY KEY (domain, url)
);

CREATE TABLE pages_by_userid 
(
    user_id int,
    user_text text,
    url text,
    date timestamp,
    PRIMARY KEY (user_id, date, url)
);

CREATE TABLE pageids 
(
    page_id int,
    url text,
    PRIMARY KEY (page_id, url)
);

CREATE TABLE category_a_1
(
    time_start int PRIMARY KEY,
    time_end int,
    statistics map<text, int> 
);

CREATE TYPE stats 
(
    domain text,
    created_by_bots int
);

CREATE TABLE category_a_2
(
    time_start int PRIMARY KEY,
    time_end int,
    statistics list<frozen<stats>> 
);