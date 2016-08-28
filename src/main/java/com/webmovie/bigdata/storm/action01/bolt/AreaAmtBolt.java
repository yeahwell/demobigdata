package com.webmovie.bigdata.storm.action01.bolt;


import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AreaAmtBolt implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Map <String,Double> countsMap = null ;
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

		countsMap.clear() ;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// area_id,count
 
		if (input != null) {
			String area_id = input.getString(0) ;
			double order_amt = 0.0;
			try {
				order_amt = input.getDouble(1) ;
			} catch (Exception e) {
				System.out.println(input.getString(1)+":---------------------------------");
				e.printStackTrace() ;
			}
			
			String order_date = input.getStringByField("order_date") ;
			
			Double count = countsMap.get(order_date+"_"+area_id) ;
			if (count == null) {
				count = 0.0 ;
			}
			count += order_amt ;
			countsMap.put(order_date+"_"+area_id, count) ;
			System.err.println("areaAmtBolt:"+order_date+"_"+area_id+"="+count);
			collector.emit(new Values(order_date+"_"+area_id,count)) ;
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		countsMap = new HashMap<String, Double>() ;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

		declarer.declare(new Fields("date_area","amt")) ;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
