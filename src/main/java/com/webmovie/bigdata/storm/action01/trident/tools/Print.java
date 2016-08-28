package com.webmovie.bigdata.storm.action01.trident.tools;


import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class Print extends BaseFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
//		System.err.println(tuple);
		if (! tuple.isEmpty()) {
			String msg = tuple.getString(0);
			System.err.println(msg);
		}
	}
	
	

}
