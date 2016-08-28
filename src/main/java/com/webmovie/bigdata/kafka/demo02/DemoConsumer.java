package com.webmovie.bigdata.kafka.demo02;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class DemoConsumer extends Thread {
	
	private final ConsumerConnector consumer;
	private final String topic;

	public DemoConsumer(String topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", KafkaPropertiesDemo02.zkConnect);
		props.put("group.id", KafkaPropertiesDemo02.groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "60000");//

		return new ConsumerConfig(props);

	}
// push消费方式，服务端推送过来。主动方式是pull
	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		
		while (it.hasNext()){
			//逻辑处理
			System.out.println("consumer:"+new String(it.next().message()));
			
		}
			
	}

	public static void main(String[] args) {
		DemoConsumer consumerThread = new DemoConsumer(KafkaPropertiesDemo02.topic);
		consumerThread.start();
	}
	
}
