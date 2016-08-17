package com.webmovie.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase客户端
 * @author yeahwell
 *
 */
public class HBaseClient {
	
	public void createNamespace(){
		
	}

	/**
	 * 1. 创建表
	 */
	public void createHBaseTable(){
		//从项目的classpath中加载配置文件
		Configuration conf = HBaseConfiguration.create();
		//System.out.println(conf.get("fs.defaultFS"));
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			
			//1. 创建namespace
			NamespaceDescriptor descriptor = NamespaceDescriptor.create("namespace1")
					.addConfiguration("createor", "beifeng")
					.addConfiguration("createTime", System.currentTimeMillis() + "")
					.build();
			admin.createNamespace(descriptor);
			
			//2. 创建表
			TableName name = TableName.valueOf(Bytes.toBytes("namespace1:test"));
			HTableDescriptor tableDesc = new HTableDescriptor(name);
			//指定表的列簇
			HColumnDescriptor columnDesc1 = new HColumnDescriptor(Bytes.toBytes("cf1"));
			//设置列簇单元格可以有最大10个版本，最小保留2个版本，版本存活时间为100妙
			columnDesc1.setMaxVersions(10);
			columnDesc1.setMinVersions(2);
			columnDesc1.setTimeToLive(100);
			//HBase支持SNAPPY压缩：
			//columnDesc1.setCompressionType(Algorithm.SNAPPY);
			columnDesc1.setCompressionType(Algorithm.GZ);
			tableDesc.addFamily(columnDesc1);
			
			//指定表的列簇
			HColumnDescriptor columnDesc2 = new HColumnDescriptor(Bytes.toBytes("cf2"));
			//设置列簇单元格可以有最大10个版本，最小保留2个版本，版本存活时间为100妙
			columnDesc2.setMaxVersions(10);
			columnDesc2.setMinVersions(2);
			columnDesc2.setTimeToLive(100);
			//HBase支持SNAPPY压缩：
			columnDesc2.setCompressionType(Algorithm.GZ);
			tableDesc.addFamily(columnDesc2);
			
			admin.createTable(tableDesc);
			
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(null != admin){
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) {
		HBaseClient client = new HBaseClient();
		client.createHBaseTable();
	}
	
}
