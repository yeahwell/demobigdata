package com.webmovie.bigdata.mapreduce.wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, LongWritable>{

	private Text word = new Text();
	private LongWritable one = new LongWritable(1);
	
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		System.out.println("[WordCountMapper] map...");
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			context.write(word, one);
		}
	}

	@Override
	protected void setup(
			Mapper<Object, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		System.out.println("[WordCountMapper] 调用setup方法");
	}

	@Override
	protected void cleanup(
			Mapper<Object, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		System.out.println("[WordCountMapper] 调用cleanup方法");
	}
	
}
