package com.webmovie.bigdata.kafka.demo02;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class DemoProducer extends Thread {
	private final kafka.javaapi.producer.Producer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();

	public DemoProducer(String topic) {
		props.put("serializer.class", "kafka.serializer.StringEncoder");// 字符串消息
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
		for (int i = 0; i < 2000; i++) {
			String messageStr = new String("Message_" + i);
			System.out.println("product:"+messageStr);
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
		}

	}

	public static void main(String[] args) {
		DemoProducer producerThread = new DemoProducer(KafkaPropertiesDemo02.topic);
		producerThread.start();
	}
}