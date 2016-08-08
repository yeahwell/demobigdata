package com.webmovie.bigdata.mapreduce.friend;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * 3个User
 * 
 * 	A	B	2
 *  A	C	1
 *  C	D	3
 * 
 * @author ad
 *
 */
public class BestFOFSorter extends WritableComparator {
	
	public BestFOFSorter(){
		// true -- 创建实例
		super(User.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		User usera = (User) a;
		User userb = (User) b;
		
		int result = usera.getUserName().compareTo(userb.getUserName());
		if(result== 0){
			// 降序排
			return - Integer.compare(usera.getWeight(), userb.getWeight());
		}
		
		return  result;
	}
}