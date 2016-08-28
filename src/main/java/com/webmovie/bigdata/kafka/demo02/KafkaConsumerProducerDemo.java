package com.webmovie.bigdata.kafka.demo02;

public class KafkaConsumerProducerDemo implements KafkaPropertiesDemo02 {

	public static void main(String[] args) {
		DemoProducer producerThread = new DemoProducer(KafkaPropertiesDemo02.topic);
		producerThread.start();

		DemoConsumer consumerThread = new DemoConsumer(KafkaPropertiesDemo02.topic);
		consumerThread.start();

	}
}