package com.webmovie.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class TestHBaseAdmin {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseUtil.getHBaseConfiguration();
		HBaseAdmin admin = new HBaseAdmin(conf);
		try{
			test_createTable(admin);
//			test_getTableDesc(admin);
//			test_deleteTable(admin);
		}finally{
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建表
	 * @param admin
	 * @throws Exception
	 */
	public static void test_createTable(HBaseAdmin admin) throws Exception{
		TableName tableName = TableName.valueOf("users");
		HTableDescriptor htd = new HTableDescriptor(tableName);
		htd.addFamily(new HColumnDescriptor("f"));
		htd.setMaxFileSize(10000L);
		
		admin.createTable(htd); //执行表的创建
		System.out.println("创建表成功");
	}
	
	public static void test_getTableDesc(HBaseAdmin admin) throws IOException{
		//admin.createNamespace(NamespaceDescriptor.create("ns2").build());
		TableName name = TableName.valueOf("users");
		HTableDescriptor htd = admin.getTableDescriptor(name);
		System.out.println(htd);
	}
	
	/**
	 * 测试删除
	 * 
	 * @param hbAdmin
	 * @throws IOException
	 */
	public static void test_deleteTable(HBaseAdmin hbAdmin) throws IOException {
		TableName name = TableName.valueOf("users");
		if (hbAdmin.tableExists(name)) {// 判断表是否存在
			if (hbAdmin.isTableEnabled(name)) {// 判断表的状态是出于enabled还是disabled状态。
				hbAdmin.disableTable(name);
			}
			hbAdmin.deleteTable(name);
			System.out.println("删除成功");
		} else {
			System.out.println("表不存在");
		}
	}
	
}
