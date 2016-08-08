package com.webmovie.bigdata.mapreduce.friend;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * 
 * 
 * 输入：
 * 
 * 	user   list(A:5,B:2,C:1)
 * 
 * 
 *D A:5,B:2,C:1
 * @author ad
 *
 */
public class BestFOFReducer extends Reducer<User, Text, Text, Text>{
	
	
	@Override
	protected void reduce(User user, Iterable<Text> values, 
			Reducer<User, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuilder sbuilder = new StringBuilder();
		int i = 0;
		for(Text v:values){
			
			if(i == 0){
				sbuilder.append(v.toString());
			}else{
				sbuilder.append(","+v.toString());
			}
			i++;
		}
		
		context.write(new Text(user.getUserName()), new Text(sbuilder.toString()));
	}
}
