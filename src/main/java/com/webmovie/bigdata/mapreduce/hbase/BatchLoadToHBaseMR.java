package com.webmovie.bigdata.mapreduce.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class BatchLoadToHBaseMR implements Tool{

	/**
	 * 仅仅只是把输入的KEY,VALUE声明好
	 * @author yeahwell
	 *
	 */
	static class ReadDataFromHBaseMapper extends TableMapper<Text, Text>{

		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//key --> rowkey
			//value --> result
			String rowkey = Bytes.toString(key.get());
			
			//微博内容表只有一个单元格
			Cell[] cellArray = value.rawCells();
			for(Cell cell : cellArray){
				String content = Bytes.toString(CellUtil.cloneValue(cell));
				context.write(new Text(rowkey), new Text(content));
			}
		}
	}

	@Override
	public void setConf(Configuration conf) {
	}

	@Override
	public Configuration getConf() {
		return null;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hfds://beifeng-hadoop-02:9000");
		conf.set("hbase.rootdir", "hfds://beifeng-hadoop-02:9000/hbase");
		conf.set("hbase.zookeeper.quorum", "beifeng-hadoop-02"); // hbase zk环境信息
		
		System.setProperty("HADOOP_USER_NAME", "beifeng");
		
		Job job = Job.getInstance();
		job.setJobName("batchLoadToHBase");
		job.setJarByClass(BatchLoadToHBaseMR.class);
		
		Path outputDir = new Path("");
		
		//先要生成HFile
		HTable table = new HTable(conf, TableName.valueOf(Bytes.toBytes("weibo:content")));
		HFileOutputFormat2.configureIncrementalLoad(job, table);
		
		return 0;
	}
	
}
