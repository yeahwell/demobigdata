package com.webmovie.bigdata.storm.action01.kafka;

public interface KafkaPropertiesAction01
{
  final static String zkConnect = "beifeng-hadoop-02:2181";
  final static  String groupId = "group1";
  final static String topic = "track";
  final static String Order_topic = "track";
  
//  final static String kafkaServerURL = "localhost";
//  final static int kafkaServerPort = 9092;
//  final static int kafkaProducerBufferSize = 64*1024;
//  final static int connectionTimeOut = 100000;
//  final static int reconnectInterval = 10000;
//  final static String topic2 = "topic2";
//  final static String topic3 = "topic3";
//  final static String clientId = "SimpleConsumerDemoClient";
}