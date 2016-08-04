package com.webmovie.bigdata.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

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


public class WCMapReduceTemplate extends Configured implements Tool{

	/**
	 * step 1 Mapper Class
	 * @author webmovie
	 * Map过程需要继承org.apache.hadoop.mapreduce包中Mapper类，并重写其map方法。
	 * 通过在map方法中添加两句把key值和value值输出到控制台的代码，
	 * 可以发现map方法中value值存储的是文本文件中的一行（以回车符为行结束标记），
	 * 而key值为该行的首字母相对于文本文件的首地址的偏移量。
	 * 然后StringTokenizer类将每一行拆分成为一个个的单词，并将<word,1>作为map方法的结果输出，其余的工作都交有MapReduce框架处理。
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private Text mapOutputKey = new Text();
		private final static IntWritable mapOutputValue = new IntWritable(1);

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("WCMapper {key=" + key + ",value=" + value + "}");
			
			//line value
			String lineValue = value.toString();
			
			StringTokenizer itr = new StringTokenizer(lineValue);
			while(itr.hasMoreTokens()){
				mapOutputKey.set(itr.nextToken());
				context.write(mapOutputKey, mapOutputValue);
			}
			
//			//split
//			String[] strArray = lineValue.split(" ");
//			//iterator
//			for(String str : strArray){
//				//set map output key
//				mapOutputKey.set(str);
//				//output
//				context.write(mapOutputKey, mapOutputValue);
//				System.out.println("<" + mapOutputKey + "," + mapOutputValue + ">");
//			}
			
		}
		
	}
	
	/**
	 * step 2 Reducer Class
	 * @author webmovie
	 * Reduce过程需要继承org.apache.hadoop.mapreduce包中Reducer类，并重写其reduce方法。
	 * Map过程输出<key,values>中key为单个单词，而values是对应单词的计数值所组成的列表，
	 * Map的输出就是Reduce的输入，所以reduce方法只要遍历values并求和，即可得到某个单词的总次数。
	 */
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			System.out.println("WCReducer {key=" + key + ",value=" + values + "}");
			
			//temp sum
			int sum = 0;
			
			//iterator
			for(IntWritable value : values){
				//sum
				sum += value.get();
			}
			
			//set output value
			outputValue.set(sum);
			
			//output
			context.write(key, outputValue);
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		//set jarByClass
		job.setJarByClass(this.getClass());
		
		//mapper
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//combiner
		job.setCombinerClass(WCReducer.class);
		
		//reducer
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//set job input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//output input
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//submit job
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		args = new String[]{
				"hdfs://beifeng-hadoop-02:9000/user/beifeng/mapreduce/wc_in",
				"hdfs://beifeng-hadoop-02:9000/user/beifeng/mapreduce/wc_out01"
		};
		Configuration conf = new Configuration();
		//conf.set("mapreduce.map.output.compress", "true");
		//compress 
		//conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		
		int status = ToolRunner.run(conf, new WCMapReduceTemplate(), args);
		System.exit(status);
	}

	
}
