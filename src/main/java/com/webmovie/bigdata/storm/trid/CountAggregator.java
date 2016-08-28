package com.webmovie.bigdata.storm.trid;

import java.util.Map;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.webmovie.bigdata.storm.trid.CountAggregator.CountState;

public class CountAggregator implements Aggregator<CountState>{

	static class CountState{
		long count = 0L;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public CountState init(Object batchId, TridentCollector collector) {
		return new CountState();
	}

	/**
	 * 同一批次内各个分区有多少Tuple，则调用该方法多少次
	 */
	@Override
	public void aggregate(CountState val, TridentTuple tuple,
			TridentCollector collector) {
		long oldCount = val.count;
		long newCount = oldCount + 1;
		val.count = newCount;
	}

	@Override
	public void complete(CountState val, TridentCollector collector) {
		
	}
	
	
	
}
