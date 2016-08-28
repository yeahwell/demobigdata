package com.webmovie.bigdata.storm.trid;

import java.util.List;
import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class HasFlumeFilter implements Filter {

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		String str = tuple.getStringByField("str");
		//System.out.println("PrintTestFilter: " + str);
		if(str.contains("flume")){
			System.out.println("HasFlumeFilter: " + str);
			return true;
		}
		return false;
	}

}
