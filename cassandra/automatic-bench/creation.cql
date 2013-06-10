CREATE KEYSPACE usertable WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE usertable;

CREATE TABLE usertable (
    key varchar PRIMARY KEY,
    field0 varchar,
    field1 varchar,
    field2 varchar,
    field3 varchar,
    field4 varchar,
    field5 varchar,
    field6 varchar,
    field7 varchar,
    field8 varchar,
    field9 varchar,
    field10 varchar
);
