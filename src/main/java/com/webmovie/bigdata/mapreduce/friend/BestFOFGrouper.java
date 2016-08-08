package com.webmovie.bigdata.mapreduce.friend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 自定义分组
 * @author ad
 *
 */
public class BestFOFGrouper extends WritableComparator {
	
	public BestFOFGrouper(){
		// true -- 创建实例
		super(User.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		User usera = (User) a;
		User userb = (User) b;
		
		return usera.getUserName().compareTo(userb.getUserName());
	}
}
