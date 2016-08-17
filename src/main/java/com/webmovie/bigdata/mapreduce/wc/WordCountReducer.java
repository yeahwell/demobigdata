package com.webmovie.bigdata.mapreduce.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	private LongWritable count = new LongWritable();
	
	@Override
	protected void cleanup(
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		System.out.println("[WordCountReducer] 调用cleanup方法");

	}
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context)
			throws IOException, InterruptedException {
		System.out.println("[WordCountReducer] reduce...");
		long sum = 0;
		for(LongWritable value : values){
			sum += value.get();
		}
		count.set(sum);
		context.write(key, count);
	}
	
	@Override
	protected void setup(
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		System.out.println("[WordCountReducer] 调用setup方法");

	}
	
}
