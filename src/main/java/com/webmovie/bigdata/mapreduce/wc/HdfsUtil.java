package com.webmovie.bigdata.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtil {

	public static FileSystem getFileSystem() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://beifeng-hadoop-02:9000");
		FileSystem fileSystem = FileSystem.get(configuration);
		return fileSystem;
	}

	public static boolean deleteFile(String path) throws Exception {
		FileSystem fs = getFileSystem();
		try {
			return fs.delete(new Path(path), true);
		} finally {
			fs.close();
		}
	}

}
