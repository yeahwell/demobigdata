package com.webmovie.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Sort extends Configured implements Tool{

	public static class MyMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		
		private static IntWritable data = new IntWritable();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
		
	}
	
	public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		
		private static IntWritable linenum = new IntWritable(1);

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			for(IntWritable val : values){
				context.write(linenum, key);
				linenum = new IntWritable(linenum.get() + 1);
			}
		}
		
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		//set jarByClass
		job.setJarByClass(this.getClass());
		
		//mapper
		job.setMapperClass(MyMapper.class);
		//reducer
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//set job input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//output input
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//submit job
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://beifeng-hadoop-02:9000/user/beifeng/mapreduce/sort_in",
				"hdfs://beifeng-hadoop-02:9000/user/beifeng/mapreduce/sort_out05"
		};
		Configuration conf = new Configuration();
		
		int status = ToolRunner.run(conf, new Sort(), args);
		System.exit(status);
	}
	
}
