package com.webmovie.bigdata.storm.trid;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class CountFunction implements Function {
	
	private Map<String, Long> wordcounts;
	
	private Integer partitionIndex;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		wordcounts = new HashMap<String, Long>();
		
		partitionIndex = context.getPartitionIndex();
		System.err.println("CountFunction的分区编号 " + partitionIndex);
		
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String word = tuple.getStringByField("word");
		Long counts = 0L;
		if(wordcounts.containsKey(word)){
			counts = wordcounts.get(word);
		}
		counts += 1;
		wordcounts.put(word, counts);
		
		for(String key : wordcounts.keySet()){
			
//			collector.emit(new Values(key));
			
			System.out.println("partitionIndex=" + partitionIndex + ", word=" + word + ", count=" + counts);
		}
	}

}
