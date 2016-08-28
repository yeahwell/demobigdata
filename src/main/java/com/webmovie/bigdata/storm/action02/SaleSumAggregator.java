package com.webmovie.bigdata.storm.action02;

import java.util.Map;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class SaleSumAggregator implements Aggregator<SaleSumState> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5624187901867971903L;
	
	private int partitionIndex;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public SaleSumState init(Object batchId, TridentCollector collector) {
		return new SaleSumState();
	}

	@Override
	public void aggregate(SaleSumState val, TridentTuple tuple,
			TridentCollector collector) {
		double oldSaleSum = val.saleSum;
		double price = tuple.getDoubleByField("price");
		double newSaleSum = oldSaleSum + price;
		val.saleSum = newSaleSum;
	}

	@Override
	public void complete(SaleSumState val, TridentCollector collector) {
		System.err.println("SaleSum --> partitonIndex=" + partitionIndex
				+ ", saleSum=" + val.saleSum);
		collector.emit(new Values(val.saleSum));
	}

}
