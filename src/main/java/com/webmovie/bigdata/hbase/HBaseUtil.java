package com.webmovie.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseUtil {

	public static Configuration getHBaseConfiguration(){
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "beifeng-hadoop-02");
		return conf;
	}
	
	
	
}
