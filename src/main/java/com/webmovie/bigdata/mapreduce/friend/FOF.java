package com.webmovie.bigdata.mapreduce.friend;

import org.apache.hadoop.io.Text;

/**
 * 好友对（好友的好友）对象
 * 自定义key类型
 * @author webmovie
 *
 */
public class FOF extends Text{
	
	public FOF(){
		super();
	}
	
	public FOF(String userA,String userB){
		super(joinUser(userA, userB));
	}
	
	//同一好友对的写法
	public static String joinUser(String userA,String userB){
		if(userA.compareTo(userB) > 0){
			return userA + "\t" + userB;
		}
		return userB + "\t" + userA;
	}

}
