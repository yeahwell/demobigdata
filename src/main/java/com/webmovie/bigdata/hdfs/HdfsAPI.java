package com.webmovie.bigdata.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class HdfsAPI {
	
	@Test
	public void test_createFile01() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		boolean created = fileSystem.createNewFile(new Path("/user/yeahwell/abc01.txt"));
		System.out.println(created ? "创建成功" : "创建失败");
		fileSystem.close();
	}
	
	@Test
	public void test_createFile02() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		boolean created = fileSystem.createNewFile(new Path("abc02.txt"));
		System.out.println(created ? "创建成功" : "创建失败");
		fileSystem.close();
	}
	
	@Test
	public void test_append() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		Path path = new Path("/user/yeahwell/abc01.txt");
		FSDataOutputStream os  = fileSystem.append(path);
		os.write("hadoop hdfs实战".getBytes());
		fileSystem.close();
	}
	
	@Test
	public void test_createAndAppend() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		Path path = new Path("/user/yeahwell/abc03.txt");
		FSDataOutputStream os  = fileSystem.create(path);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
		bw.write("hdfs create and write data to file");
		bw.newLine();
		bw.write("离线数据分析平台");
		
		bw.close();
		os.close();
		fileSystem.close();
		
	}
	
	@Test
	public void test_createAndAppend2() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		Path path = new Path("/user/yeahwell/abc04.txt");
		FSDataOutputStream os  = fileSystem.create(path);
		//设置replication
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os), (short)1);
		bw.write("hdfs create and write data to file");
		bw.newLine();
		bw.write("离线数据分析平台");
		
		bw.close();
		os.close();
		fileSystem.close();
		
	}
	
	@Test
	public void test_open() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		Path path = new Path("/user/yeahwell/abc04.txt");
		InputStream is = fileSystem.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line = null;
		while((line = br.readLine()) != null) {
			System.out.println(line);
		}
		
		br.close();
		is.close();
		fileSystem.close();
	}
	
	@Test
	public void test_mkdirs() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		Path path = new Path("/user/yeahwell/mydir");
		boolean result = fileSystem.mkdirs(path);
		System.out.println(result);
		fileSystem.close();
	}
	
	@Test
	public void test_copyFromLocal() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		fileSystem.copyFromLocalFile(new Path("/Users/yeahwell/Customer.txt"), new Path("/user/yeahwell/abc05.txt"));
		fileSystem.close();
	}
	
	@Test
	public void test_copyToLocal() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		fileSystem.copyToLocalFile(new Path("/user/yeahwell/abc05.txt"), new Path("/Users/yeahwell/abc05.txt"));
		fileSystem.close();
	}
	
	@Test
	public void test_delete() throws Exception{
		FileSystem fileSystem = HdfsUtil.getFileSystem();
		
		boolean result01 = fileSystem.delete(new Path("/user/yeahwell/abc05.txt"), true);
		System.out.println("递归删除" + result01);
		
		boolean result09 = fileSystem.deleteOnExit(new Path("/user/yeahwell/abc03.txt"));
		System.out.println("deleteOnExit : " + result09);	
		
		System.in.read();
		
		boolean result02 = fileSystem.delete(new Path("/user/yeahwell/mydir"), false);
		System.out.println("非递归删除" + result02);
		
		boolean result03 = fileSystem.delete(new Path("/user/yeahwell/mydir"), true);
		System.out.println("非递归删除" + result03);
		
		fileSystem.close();
	}
	
	@Test
	public void test_fileStatus() throws Exception{
		FileSystem fs = HdfsUtil.getFileSystem();
		FileStatus status = fs.getFileStatus(new Path("/user/yeahwell/abc01.txt"));
		System.out.println(status.isDirectory() ? "是文件夹" : "是文件");
		System.out.println("提交时间:" + status.getAccessTime());
		System.out.println("复制因子:" + status.getReplication());
		System.out.println("长度:" + status.getLen());
		System.out.println("最后修改时间:" + status.getModificationTime());
		fs.close();
	}
}
