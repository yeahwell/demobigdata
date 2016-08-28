package com.webmovie.bigdata.storm.action01.bolt;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.webmovie.bigdata.storm.action01.kafka.DateFmt;

public class AreaFilterBolt implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// order_id,order_amt,create_time,area_id

		String order = input.getString(0) ;
		if (order != null) {
			String orderArr[] = order.split("\\t") ;
			collector.emit(new Values(orderArr[3],orderArr[1],DateFmt.getCountDate(orderArr[2], DateFmt.date_short))) ;// ared_id,order_amt,create_time
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("area_id","order_amt","order_date")) ;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
