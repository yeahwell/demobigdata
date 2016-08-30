package com.webmovie.bigdata.storm.action02;

import java.util.Map;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * 为了方便后的统计结果存储，需要将多字段进行拼接
 * @author yeahwell
 *
 */
public class CombineKeyFunction implements Function {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5524423398970386821L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String yyyyMMddHHStr = tuple.getStringByField("yyyyMMddHHStr");
		String country = tuple.getStringByField("country");
		String province = tuple.getStringByField("province");
		String city = tuple.getStringByField("city");
		
		//rowkey 从左到右按字典顺序排序
		String newkey = city + "_" + province + "_" + country + "_" + yyyyMMddHHStr;
		collector.emit(new Values(newkey));
		
	}

}
