package com.webmovie.bigdata.hdfs;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsApp {

	public static FileSystem getFileSystem() throws Exception{
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		return fileSystem;
	}
	
	public static void read(String filename) throws Exception{
		FileSystem fileSystem = getFileSystem();
		
		Path readPath = new Path(filename);
		FSDataInputStream inStream = fileSystem.open(readPath);
		IOUtils.copyBytes(inStream, System.out, 4096, false);
		
	}
	
	public static void main(String[] args) throws Exception{
		
		String filename = "/user/beifeng/input/abc.txt";
		read(filename);
		
		FileSystem fileSystem = getFileSystem();
		System.out.println(fileSystem);
		
		Path readPath = new Path(filename);
		FSDataOutputStream outStream = fileSystem.create(readPath);
		FileInputStream inStream = new FileInputStream(new File("/opt/datas/20160801/abc.txt"));
		
		//copy local file to hdfs
		IOUtils.copyBytes(inStream, outStream, 4096, false);
		
		IOUtils.closeStream(inStream);
		IOUtils.closeStream(outStream);
		
	}
	
}
