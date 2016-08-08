package com.webmovie.bigdata.mapreduce.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *从用户的好友列表中筛选出直接好友和间接好友
 * 
 * A	B	H
 * 输出
 * 
 * A	B	2 #直接好友
 * B	H	1 #间接好友
 * 
 * @author ad
 *
 */
public class FindFOFMapper extends Mapper<Text,Text,FOF,IntWritable>{
	
	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] friends = value.toString().split("\t");
		
		String userA = key.toString();
		for( int i = 0 ; i < friends.length ; i ++){
			
			// 筛选并输出直接好友
			context.write(new FOF(userA,friends[i]), new IntWritable(2));
			
			for(int j = i  + 1 ; j < friends.length ; j++){
				// 筛选并输出间接好友
				context.write(new FOF(friends[i],friends[j]), new IntWritable(1));
			}
			
		}
	}
	
	
}
