package com.webmovie.bigdata.storm.action01.trident.tools;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.webmovie.bigdata.storm.action01.kafka.DateFmt;

public class OrderSplit extends BaseFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String patten = null;
	public OrderSplit(String patten)
	{
		this.patten = patten ;
	}
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		System.err.println(tuple);
		if (! tuple.isEmpty()) {
			String msg = tuple.getString(0);
			String value[] = msg.split(this.patten) ;
			System.err.println("msg="+msg);
			collector.emit(new Values(value[0],Double.parseDouble(value[1]),DateFmt.getCountDate(value[2], DateFmt.date_short),value[3]));
//			System.err.println(msg);
		}
	}
	
	

}
