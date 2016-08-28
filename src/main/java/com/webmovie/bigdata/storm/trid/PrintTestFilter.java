package com.webmovie.bigdata.storm.trid;

import java.util.List;
import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class PrintTestFilter implements Filter {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Integer partitionIndex;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		
		partitionIndex = context.getPartitionIndex();
		
	}

	@Override
	public void cleanup() {
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		List<Object> valueList = tuple.getValues();
		StringBuilder sb = new StringBuilder();
		sb.append("PrintTestFilter: ");
//		sb.append("partitionIndex=" + partitionIndex + ",  ");
		int i = 0;
		for(Object value : valueList){
			if(i == 0){
				sb.append(value);
			}else{
				sb.append("," + value);
			}
			i++;
		}
		System.out.println(sb.toString());
		return true;
	}

}
