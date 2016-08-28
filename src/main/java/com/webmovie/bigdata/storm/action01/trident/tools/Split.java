package com.webmovie.bigdata.storm.action01.trident.tools;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class Split extends BaseFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String patten = null;
	public Split(String patten)
	{
		this.patten = patten ;
	}
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
//		System.err.println(tuple);
		if (! tuple.isEmpty()) {
			String msg = tuple.getString(0);
			String value[] = msg.split(this.patten) ;
			for (String v : value) {
				collector.emit(new Values(v));
			}
		}
	}
	
	

}