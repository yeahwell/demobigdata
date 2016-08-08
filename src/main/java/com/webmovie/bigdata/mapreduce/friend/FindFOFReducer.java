package com.webmovie.bigdata.mapreduce.friend;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 排除不是真正间接好友的fof
 * 统计间接好友同时出现的次数
 * 
 * 输入：
 * 	一种情况： key( B	H ) value = List(2,1,1,1)
 * 		B	H 	2
 * 		B	H	1
 * 		B	H	1
 * 		B	H	1
 * 
 * 	第二种情况：key( B	H ) value = List(1,1,1)
 * 		B	H	1
 * 		B	H	1
 * 		B	H	1
 * 
 * 输出：
 * 	B	H	3
 * @author ad
 *
 */
public class FindFOFReducer extends Reducer<FOF,IntWritable, FOF, IntWritable>{
	
	@Override
	protected void reduce(FOF key, Iterable<IntWritable> values,
			Reducer<FOF, IntWritable, FOF, IntWritable>.Context context) 
					throws IOException, InterruptedException {
		
		int sum  = 0 ; 
		
		boolean isRealFOF = true;
		for(IntWritable v  : values){
			// 判断是否有2标识
			int count = v.get();
			if(count == 2){
				// 表示这个fof是直接好友
				isRealFOF = false;
				break;
			}
			// 对间接好友，将次数累加
			sum  += v.get();
		}
		// 对是真正的间接好友，将累加结果输出到结果文件里面
		if(isRealFOF) context.write(key, new IntWritable(sum));
	}
	

}
