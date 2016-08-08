package com.webmovie.bigdata.mapreduce.friend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 找到前几名应该推荐的好友
 * 
 * 对间接好友出现的次数进行排序
 * 
 * 输入： C A 1 ---> userA : A C 1 userB : c a 1 G A 1 G C 2
 * 
 * 
 * 针对每个用户： 对它的间接好友出现次数进行排序
 * 
 * A C 1 A D 2
 * 
 * 
 * 
 * 
 * job最终的结果想达到这个效果 A D:2,C:1
 * 
 * 
 * 
 * @author ad
 *
 */
public class BestFOFJobEntry implements Tool {

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

		Job job = Job.getInstance(conf, "bestfof-job");

		// 指定按tab拆分一行记录，tab左边作为key，右边内容作为value值
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setJarByClass(BestFOFJobEntry.class);

		job.setMapperClass(BestFOFMapper.class);

		job.setMapOutputKeyClass(User.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setPartitionerClass(BestFOFPartitioner.class);
		job.setSortComparatorClass(BestFOFSorter.class);
		job.setGroupingComparatorClass(BestFOFGrouper.class);

		job.setReducerClass(BestFOFReducer.class);

		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path("/user/beifeng/mapreduce/findfof_out/"));

		// 输出文件
		Path outdir = new Path("/user/beifeng/mapreduce/bestfof_out/");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outdir)) {
			fs.delete(outdir, true);
		}
		FileOutputFormat.setOutputPath(job, outdir);
		boolean isSuccess = job.waitForCompletion(true);
		// 判断是否达到收敛条件，如果达到就中断
		if (isSuccess) {
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new BestFOFJobEntry(), args);
		if (exitcode == 0) {
			System.out.println("successfully!!!!");
		}
	}
}