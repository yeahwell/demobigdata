package com.webmovie.bigdata.storm.action01.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.webmovie.bigdata.storm.action01.hbase.dao.HBaseDao;
import com.webmovie.bigdata.storm.action01.hbase.dao.impl.HBaseDaoImpl;

public class AreaRsltBolt implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Map <String,Double> countsMap = null ;
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}
	HBaseDao dao = null;
	long beginTime = System.currentTimeMillis() ;
	long endTime = 0L ;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub

		String date_areaid = input.getString(0) ;
		double order_amt = input.getDouble(1) ;
		countsMap.put(date_areaid, order_amt) ;
		
		endTime = System.currentTimeMillis() ;
		if (endTime - beginTime >= 5 * 1000) {
			for(String key : countsMap.keySet())
			{
				// put into hbase
				// 2014-05-05_1,amt
				dao.insert("area_order", key, "cf", "order_amt", countsMap.get(key)+"") ;
				System.err.println("rsltBolt put hbase: key="+key+"; order_amt="+countsMap.get(key));
			}
			beginTime = System.currentTimeMillis() ;
		}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

		dao = new HBaseDaoImpl() ;
		countsMap = new HashMap<String, Double>() ;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
