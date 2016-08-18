package com.webmovie.bigdata.mapreduce.ri;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReverseIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		StringBuffer sb = null;
		Map<String, Integer> map = new HashMap<String, Integer>();
		for(Text value : values){
			sb = new StringBuffer();
			String line = value.toString();
			String[] strArray = sb.append(line).reverse().toString().split(":", 2);
			String path = sb.delete(0, sb.length() - 1).append(strArray[1]).reverse().toString();
			int count = Integer.valueOf(strArray[0]);
			if(map.containsKey(path)){
				map.put(path, map.get(path) + count);
			}else{
				map.put(path, count);
			}
		}
		
		sb = new StringBuffer();
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
		}
		outputValue.set(sb.deleteCharAt(sb.length() - 1).toString());
	}
	
}
