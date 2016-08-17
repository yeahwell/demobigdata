package com.webmovie.bigdata.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;


public class WordCountRunner implements Tool{

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
