package com.webmovie.bigdata.mapreduce.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 找到好友的好友
 * 
 * 并且统计间接好友出现的次数
 * 
 * @author ad
 *
 */
public class FindFOFJobEntry implements Tool {

	@Override
	public void setConf(Configuration conf) {

	}

	@Override
	public Configuration getConf() {

		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", "hdfs://beifeng-hadoop-02:9000");

		// 设置系统变量，解决hdfs权限问题
		System.setProperty("HADOOP_USER_NAME", "beifeng");

		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {

		// 构建job
		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "findfof-job");

		// 指定按tab拆分一行记录，tab左边作为key，右边内容作为value值
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setJarByClass(FindFOFJobEntry.class);

		job.setMapperClass(FindFOFMapper.class);

		job.setMapOutputKeyClass(FOF.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(FindFOFReducer.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path("/user/beifeng/mapreduce/findfof_in/"));

		// 输出文件
		Path outdir = new Path("/user/beifeng/mapreduce/findfof_out01/");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outdir)) {
			fs.delete(outdir, true);
		}
		FileOutputFormat.setOutputPath(job, outdir);
		boolean isSuccess = job.waitForCompletion(true);
		// 判断是否达到收敛条件，如果达到就中断
		if (isSuccess) {
			return 0;
		}else{
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new FindFOFJobEntry(), args);
		if (exitcode == 0) {
			System.out.println("successfully!!!!");
		}
	}
}
