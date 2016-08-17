package com.webmovie.bigdata.mapreduce.wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.webmovie.bigdata.mapreduce.util.HdfsUtil;

public class WordCountRunner implements Tool{
	
	public static class WordCountMapper extends Mapper<Object, Text, Text, LongWritable>{

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
	
	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

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


	private Configuration conf;
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		conf.set("fs.defaultFS", "hdfs://beifeng-hadoop-02:9000");
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		args = new String[]{
				"hdfs://beifeng-hadoop-02:9000/user/beifeng/mapreduce/wc_in",
				"hdfs://beifeng-hadoop-02:9000/user/beifeng/mapreduce/wc_out02"
		};
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wordcount");

		//1. 输入
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//2. map
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//3. shuffle
		
		//4. reduce
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//删除输出文件
		HdfsUtil.deleteFile(args[1]);
		
		//2. 输出
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//submit job
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WordCountRunner(), args);
	}

}
