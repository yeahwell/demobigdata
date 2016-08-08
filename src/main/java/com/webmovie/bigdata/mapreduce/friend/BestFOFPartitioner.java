package com.webmovie.bigdata.mapreduce.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 自定义分区
 * @author ad
 *
 */
public class BestFOFPartitioner extends HashPartitioner<User, Text>{
	
	@Override
	public int getPartition(User key, Text value, int numReduceTasks) {
		
		return (key.getUserName().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

}
