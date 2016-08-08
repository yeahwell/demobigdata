package com.webmovie.bigdata.mapreduce.friend;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输出：                                                 输出：
 * C	A	1    --->  userA :  A C 1 --> c:1    userB : c  a  1
 * @author ad
 *
 */
public class BestFOFMapper extends  Mapper<Text, Text, User, Text>{
	
	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, User, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] fofWeight = value.toString().split("\t");
		
		String userNameA = key.toString();
		String userNameB = fofWeight[0];
		
		int weight = Integer.parseInt(fofWeight[1]);
		
		User userA = new User();
		userA.setUserName(userNameA);
		userA.setFof(userNameB);
		userA.setWeight(weight);
		
		context.write(userA, new Text(userNameB+":"+weight));
		
		User userB = new User();
		userB.setUserName(userNameB);
		userB.setFof(userNameA);
		userB.setWeight(weight);
		context.write(userB, new Text(userNameA+":"+weight));
	}
	
}
