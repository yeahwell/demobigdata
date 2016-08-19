package com.webmovie.bigdata.mapreduce.shuffle;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntPairGrouping extends WritableComparator{

	public IntPairGrouping() {
		super(IntPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPair key1 = (IntPair) a;
		IntPair key2 = (IntPair) b;
		return Integer.compare(key1.getFirst(), key2.getFirst());
	}
	
}
