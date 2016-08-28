package com.webmovie.bigdata.storm.action01.kafka;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import backtype.storm.utils.Utils;



public class OrderProducer extends Thread {

	private final Producer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();

	public OrderProducer(String topic) {
		props.put("serializer.class", "kafka.serializer.StringEncoder");// 字符串消息
		//指定kafka的地址，集群以逗号隔开
		props.put("metadata.broker.list",
				"beifeng-hadoop-02:9092");
		// Use random partitioner. Don't need the key type. Just set it to
		// Integer.
		// The message is of type String.
		producer = new kafka.javaapi.producer.Producer<Integer, String>(
				new ProducerConfig(props));
		this.topic = topic;
	}

	public void run() {
		// order_id,order_amt,create_time,area_id
		Random random = new Random();
		String[] order_amt = { "10.10", "20.10", "50.2", "60.0", "80.1" };
		String[] area_id = { "1", "2", "3", "4", "5" };
		int i = 0;
		while (true) {
			i++;
			String messageStr = i + "\t" + order_amt[random.nextInt(5)] + "\t"
					+ DateFmt.getCountDate(null, DateFmt.date_long) + "\t"
					+ area_id[random.nextInt(5)];
			System.out.println("发送消息: product:" + messageStr);
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
			 Utils.sleep(5000) ;
//			if (i == 10) {
//				break;
//			}
		}

	}

	public static void main(String[] args) {
		OrderProducer producerThread = new OrderProducer(
				KafkaPropertiesAction01.Order_topic);
		producerThread.start();
	}
}
