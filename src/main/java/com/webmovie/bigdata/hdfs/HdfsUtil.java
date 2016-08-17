package com.webmovie.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsUtil {

	public static FileSystem getFileSystem() throws Exception{
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://beifeng-hadoop-02:9000");
		FileSystem fileSystem = FileSystem.get(configuration);
		return fileSystem;
	}
	
}
