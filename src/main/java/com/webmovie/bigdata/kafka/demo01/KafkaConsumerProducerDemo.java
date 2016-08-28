package com.webmovie.bigdata.kafka.demo01;

public class KafkaConsumerProducerDemo {
	
	public static void main(String[] args) {
		KafkaProducer producerThread = new KafkaProducer(KafkaPropertiesDemo01.topic);
		producerThread.start();
		KafkaConsumer consumerThread = new KafkaConsumer(KafkaPropertiesDemo01.topic);
		consumerThread.start();
	}
	
}