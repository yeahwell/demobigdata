package com.webmovie.bigdata.mapreduce.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WriteDataToHBaseMR {
	
	static class WriteDataToHBaseMapper extends Mapper<Text, Text, ImmutableBytesWritable, Put>{

		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException {
			
			String rowkey = key.toString();
			byte[] tblRowkey =  Bytes.toBytes(rowkey);
			ImmutableBytesWritable rk = new ImmutableBytesWritable(tblRowkey);
			
			Put put = new Put(tblRowkey);
			
		}
		
	}

}
