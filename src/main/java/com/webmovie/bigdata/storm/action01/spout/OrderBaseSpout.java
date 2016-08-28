package com.webmovie.bigdata.storm.action01.spout;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.webmovie.bigdata.storm.action01.kafka.OrderConsumer;

public class OrderBaseSpout implements IRichSpout {

	/**
	 * 公共基类spout
	 */
	private static final long serialVersionUID = 1L;
	
	Integer TaskId = null;
	SpoutOutputCollector collector = null;
	Queue<String> queue = new ConcurrentLinkedQueue<String>();
	String topic = null;

	public OrderBaseSpout(String topic) {
		this.topic = topic;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("order"));
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if (queue.size() > 0) {
			String str = queue.poll();
			// 进行数据过滤
			System.out.println("TaskId:" + TaskId + ";  str=" + str);
			collector.emit(new Values(str));
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		TaskId = context.getThisTaskId();
		// Thread.currentThread().getId()
		OrderConsumer consumer = new OrderConsumer(topic);
		consumer.start();
		queue = consumer.getQueue();
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
