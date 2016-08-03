package com.webmovie.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Dedup {

	//map: input value ==> output key
	public static class Map extends Mapper<Object, Text, Text, Text>{
		
		private static Text line = new Text();//each line

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			line = value;
			System.out.println("[map] value = " + line);
			context.write(line, new Text(""));
		}
	}
	
	//reduce: input key ==> output key
	public static class Reduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> arg1,
				Context context)
				throws IOException, InterruptedException {
			System.out.println("[reduce] key = " + key);
			
			context.write(key, new Text(""));
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		String[] ioArgs = new String[]{"/user/beifeng/mapreduce/dedup_in", "/user/beifeng/mapreduce/dedup_out05"};
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage data deduplication <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Data Deduplication");
		job.setJarByClass(Dedup.class);
		
		//set map/combine/reduce
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		//set output type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//set input path and output path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
}
