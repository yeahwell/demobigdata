-- 1. 创建内部表test_manager、外部表test_external以及指定location的内部表test_location
create table test_manager(id int);
create external table test_external(id int);
create table test_location(id int) location '/user/hive/warehouse/test_location';
drop table test_manager;
drop table test_external;
drop table test_location;

-- 2. 三种创建表的方式
create table customers(id int, name string, phone string) 
row format delimited fields terminated by ',' 
location '/user/hive/warehouse/customers';

hdfs dfs -put data2.txt /user/hive/warehouse/customers

create table customers2 like customers;

create table customers3 as select * from customers;

describe formatted customers;

-- 3. 复杂表的创建
create table complex_table_test(id int, name string, flag boolean, score array<int>, tech map<string, string>, other struct<phone:string,email:string>) 
row format 
delimited fields terminated by '\;' 
collection items terminated by ',' 
map keys terminated by ':' 
LOCATION '/user/hive/warehouse/complex_table_test';

hfds dfs -put data3 /user/hive/warehouse/complex_table_test

-- 4. 创建一个使用hbase的外部表
create external table hive_users(key string,id int, name string, phone string)
row format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe' 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with serdeproperties('hbase.columns.mapping'=':key,f:id,f:name,f:phone') 
tblproperties('hbase.table.name'='users');

hdfs dfs -put data3.txt /user/hive/warehouse/hive_users

-- 5. hive表维护
drop table hive_users;
truncate table hive_users;
-- 语法
alter table table_name rename to new_table_name;
alter table table_name add columns(new-cls type);
alter table table_name replace columns(new-cls type);
-- 示例
create table test(id int, name string);
alter table test rename to new_test;
describe formatted new_test;
alter table new_test add columns (age int , sex string);
describe new_test;
alter table new_test replace columns(id int, name string, age int ,sex string);

-- 6. 导入数据
-- schema on read
-- 1）从linux系统上导入数据到hive表中
-- 2）从hdfs上导入数据到hive表中
-- 3）从已有hive表到导入数据到新的hive表中

-- 7. 自定义函数
add jar /opt/jars/demobigdata-0.0.1-SNAPSHOT-jar-with-dependencies.jar;
list jar;
create temporary function hive_0901_temp AS 'com.webmovie.bigdata.hive.UDFLowerOrUpperCase';
show functions 'hive_0901.*';
create function lower_or_upper AS 'com.webmovie.bigdata.hive.UDFLowerOrUpperCase';
show functions 'lower_or_upper';

