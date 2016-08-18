package com.webmovie.bigdata.mapreduce.ri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.webmovie.bigdata.mapreduce.util.HdfsUtil;

public class ReverseIndexRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://beifeng-hadoop-02:9000");
		Job job = Job.getInstance(conf, "reverse_index");

		String inputPath = "/user/beifeng/mapreduce/ri_in";
		String outputPath = "/user/beifeng/mapreduce/ri_out01";
		
		// 1. 输入
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		
		// 2. map
		job.setMapperClass(ReverseIndexMapper.class);
		//不设置map的ouput类型，则使用reduce的output类型
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);

		// 3. shuffle

		// 4. reduce
		job.setReducerClass(ReverseIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 删除输出文件
		HdfsUtil.deleteFile(outputPath);

		// 2. 输出
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		int status = job.waitForCompletion(true) ? 0 : 1;
		
		System.out.println("执行结果" + status);
	}

}
