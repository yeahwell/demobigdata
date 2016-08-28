package com.webmovie.bigdata.storm.action02;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class OrderParseFunction implements Function {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5524423398970386821L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String orderRecord = tuple.getStringByField("str");
		if(orderRecord != null && !"".equals(orderRecord)){
			String[] orderDetailArray = orderRecord.replace("\"",  "").split(" ");
			long timestamp = Long.valueOf(orderDetailArray[0]);
			Date date = new Date(timestamp);
			DateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
			String yyyyMMddStr = yyyyMMdd.format(date);
			DateFormat yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");
			String yyyyMMddHHStr = yyyyMMddHH.format(date);
			DateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
			String yyyyMMddHHmmStr = yyyyMMddHHmm.format(date);
			
			String consumer = orderDetailArray[1];
			String productName = orderDetailArray[2];
			double price = Double.valueOf(orderDetailArray[3]);
			String country = orderDetailArray[4];
			String province = orderDetailArray[5];
			String city = orderDetailArray[6];
			
			collector.emit(new Values(timestamp, yyyyMMddStr, yyyyMMddHHStr, yyyyMMddHHmmStr,
					consumer, productName, price, country, province, city));
		}

	}

}
