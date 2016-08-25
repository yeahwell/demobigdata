-- 1. 创建内部表test_manager、外部表test_external以及指定location的内部表test_location
create table test_manager(id int);
create external table test_external(id int);
create table test_location(id int) location '/user/hive/warehouse/test_location';
drop table test_manager;
drop table test_external;
drop table test_location;

-- 2. 三种创建表的方式
create table customers(id int, name string, phone string) row format delimited fields terminated by ',' location '/user/hive/warehouse/customers';

hdfs dfs -put data2.txt /user/hive/warehouse/customers

create table customers2 like customers;

create table customers3 as select * from customers;

describe formatted customers;

-- 3. 复杂表的创建
create table complex_table_test(id int, name string, flag boolean, score array<int>, tech map<string, string>, other struct<phone:string,email:string>) row format delimited fields terminated by '\;' collection items terminated by ',' map keys terminated by ':' LOCATION '/user/hive/warehouse/complex_table_test';

hfds dfs -put data3 /user/hive/warehouse/complex_table_test

-- 4. 创建一个使用hbase的外部表
create external table hive_users(key string,id int, name string, phone string)
row format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe' 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with serdeproperties('hbase.columns.mapping'=':key,f:id,f:name,f:phone') 
tblproperties('hbase.table.name'='users');


