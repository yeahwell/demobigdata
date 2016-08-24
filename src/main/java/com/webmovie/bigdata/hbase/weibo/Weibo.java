package com.webmovie.bigdata.hbase.weibo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class Weibo {

	private static final Configuration conf = HBaseConfiguration.create();
	private static final byte[] TABLE_WEIBO_CONTENT = Bytes.toBytes("weibo:content");
	private static final byte[] TABLE_WEIBO_RELATIONS = Bytes.toBytes("weibo:relations");
	private static final byte[] TABLE_WEIBO_EMAIL = Bytes.toBytes("weibo:email");
	
	public void initNamespace(){
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			
			NamespaceDescriptor nd = NamespaceDescriptor.create("weibo")
					.addConfiguration("creator", "beifeng")
					.addConfiguration("createTime", System.currentTimeMillis() + "").build();
			admin.createNamespace(nd);
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
	
	public void initTable(){
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			//1. content
//			HTableDescriptor contentHtd = new HTableDescriptor(TableName.valueOf(TABLE_WEIBO_CONTENT));
//			HColumnDescriptor contentFamily = new HColumnDescriptor(Bytes.toBytes("cf"));
//			//开启列簇 -- store的块缓存
//			contentFamily.setBlockCacheEnabled(true);
//			contentFamily.setBlocksize(1024 * 1024 * 2);  //2M
//			contentFamily.setCompressionType(Algorithm.SNAPPY);
//			contentFamily.setMaxVersions(1);
//			contentFamily.setMinVersions(1);
//			contentHtd.addFamily(contentFamily);
//			admin.createTable(contentHtd);
			
			//2. relations
//			HTableDescriptor relationsHtd = new HTableDescriptor(TableName.valueOf(TABLE_WEIBO_RELATIONS));
//			HColumnDescriptor attend = new HColumnDescriptor(Bytes.toBytes("attend"));
//			//开启列簇 -- store的块缓存
//			attend.setBlockCacheEnabled(true);
//			attend.setBlocksize(1024 * 1024 * 2);  //2M
//			attend.setCompressionType(Algorithm.SNAPPY);
//			attend.setMaxVersions(1);
//			attend.setMinVersions(1);
//			relationsHtd.addFamily(attend);
//			HColumnDescriptor fans = new HColumnDescriptor(Bytes.toBytes("fans"));
//			//开启列簇 -- store的块缓存
//			fans.setBlockCacheEnabled(true);
//			fans.setBlocksize(1024 * 1024 * 2);  //2M
//			fans.setCompressionType(Algorithm.SNAPPY);
//			fans.setMaxVersions(1);
//			fans.setMinVersions(1);
//			relationsHtd.addFamily(fans);
//			admin.createTable(relationsHtd);
			
			//2. relations
			HTableDescriptor emailHtd = new HTableDescriptor(TableName.valueOf(TABLE_WEIBO_EMAIL));
			HColumnDescriptor emailFamily = new HColumnDescriptor(Bytes.toBytes("cf"));
			//开启列簇 -- store的块缓存
			emailFamily.setBlockCacheEnabled(true);
			emailFamily.setBlocksize(1024 * 1024 * 2);  //2M
			emailFamily.setCompressionType(Algorithm.SNAPPY);
			emailFamily.setMaxVersions(1000);
			emailFamily.setMinVersions(1000);
			emailHtd.addFamily(emailFamily);
			admin.createTable(emailHtd);
			
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
	
	/**
	 * 发布微博内容：
	 * 1）在微博内容表中插入一行数据
	 * 2）在用户微博内容接收邮件箱表，对用户的所有粉丝添加数据
	 * Put
	 * put 'tablename', 'rowkey', 'cf:cq', 'value'
	 */
	public void publishWeiboContent(String uid, String content){
		HConnection hconn = null;
		try {
			hconn = HConnectionManager.createConnection(conf);
			//1. 获取微博内容表
			HTableInterface weiboContentTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_CONTENT));
			long timestamp = System.currentTimeMillis();
			String rowkey = uid + "_" + timestamp;
			Put weiboContent = new Put(Bytes.toBytes(rowkey));
			weiboContent.add(Bytes.toBytes("cf"), Bytes.toBytes("content"), Bytes.toBytes(content));
			weiboContentTbl.put(weiboContent);
			
			//2. 查询用户关系表
			HTableInterface relationsTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_EMAIL));
			//get 'tablename', 'rowkey', 'cf', 'cq'
			Get get = new Get(Bytes.toBytes(uid));
			get.addFamily(Bytes.toBytes("fans"));
			//查询列簇下的所有粉丝
			Result r = relationsTbl.get(get);
			
			List<byte[]> fansList = new ArrayList<byte[]>();
			Cell[] cellArray = r.rawCells();
			for(Cell c : cellArray){
				fansList.add(CellUtil.cloneQualifier(c));
			}
			if(fansList.size() == 0){
				System.out.println("no fans, stop ...");
				return;
			}
			
			HTableInterface receiveContentEmailTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_EMAIL));
			List<Put> putList = new ArrayList<Put>();
			for(byte[] fanId : fansList){
				Put p = new Put(fanId);
				//p.add(Bytes.toBytes("cf"), Bytes.toBytes(uid), Bytes.toBytes(uid + "_" + System.currentTimeMillis()));
				p.add(Bytes.toBytes("cf"), Bytes.toBytes(uid), 
						timestamp,
						Bytes.toBytes(rowkey));
				putList.add(p);
			}
			receiveContentEmailTbl.put(putList);
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(hconn != null){
				try {
					hconn.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 添加关注用户
	 * 1）在weibo:content，新增数据（关注用户列簇下添加标签）
	 * 2）从被添加的关注用户角度，新增粉丝用户
	 * 3）在微博邮件箱中添加关注用户发布的微博内容通知
	 * @param args
	 */
	public void addAttends(String uid, String... attends){
		if(null == attends || attends.length <= 0){
			return;
		}
		HConnection hconn = null;
		try {
			hconn = HConnectionManager.createConnection(conf);
			//1. 在weibo:content
			HTableInterface relationsTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_RELATIONS));
			List<Put> putList = new ArrayList<Put>();
			Put put = new Put(Bytes.toBytes(uid));
			for(String attend : attends){
				put.add(Bytes.toBytes("attend"), Bytes.toBytes(attend), Bytes.toBytes(attend));
				//2. 从被添加的关注用户角度，新增粉丝用户
				Put attendPut = new Put(Bytes.toBytes(attend));
				attendPut.add(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
				putList.add(attendPut);
			}
			putList.add(put);
			relationsTbl.put(putList);
			
			//3. 
			HTableInterface weiboContentTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_CONTENT));
			List<byte[]> rks = new ArrayList<byte[]>();
			Scan scan = new Scan();
			for(String attend : attends){
				//扫描表达的rowkey，只有rowkey含有字符串（关注用户ID_)，取出
				RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(attend + "_"));
				scan.setFilter(rowFilter);
				ResultScanner scanner = weiboContentTbl.getScanner(scan);
				Iterator<Result> it = scanner.iterator();
				while(it.hasNext()){
					Result r = it.next();
					Cell[] cells =  r.rawCells();
					for(Cell c : cells){
						rks.add(CellUtil.cloneRow(c));
					}
				}
			}
			if(rks.size() == 0){
				System.out.println("no weibo Content");
				return;
			}
			
			HTableInterface receiveConentTble = hconn.getTable(TableName.valueOf(TABLE_WEIBO_EMAIL));
			List<Put> cPutList = new ArrayList<Put>();
			for(byte[] rk : rks){
				Put cp = new Put(Bytes.toBytes(uid));
				String rowkey = Bytes.toString(rk);
				long timestamp = Long.valueOf(rowkey.substring(rowkey.indexOf("_") + 1));
				String attendId  = rowkey.substring(0, rowkey.indexOf("_"));
				cp.add(Bytes.toBytes("cf"), Bytes.toBytes(attendId), 
						timestamp, rk);
				cPutList.add(cp);
			}
			receiveConentTble.put(cPutList);
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(hconn != null){
				try {
					hconn.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 1）在微博用户关系表，针对该用户，删除被取消的关注用户所对应的单元格
	 * 2）在微博用户关系表，针对被取消用户，删除它们的粉丝用户
	 * 3）在微博内容接收邮件箱表，移除该用户的这些被取消关注用户微博内容通知记录
	 * @param uid
	 * @param attends
	 */
	public void removeAttends(String uid, String... attends){
		if(null == attends || attends.length <= 0){
			return;
		}
		HConnection hconn = null;
		try {
			hconn = HConnectionManager.createConnection(conf);
			
			//1）在微博用户关系表，针对该用户，删除被取消的关注用户所对应的单元格
			HTableInterface relationsTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_RELATIONS));
			Delete deleteAttend = new Delete(Bytes.toBytes(uid));
			List<Delete> deleteFansList = new ArrayList<Delete>();
			for(String attend : attends){
				deleteAttend.deleteColumn(Bytes.toBytes("attend"), Bytes.toBytes(attend));
				//2）在微博用户关系表，针对被取消用户，删除它们的粉丝用户
				Delete deleteFans = new Delete(Bytes.toBytes(attend));
				deleteFans.deleteColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
				deleteFansList.add(deleteFans);
			}
			relationsTbl.delete(deleteAttend);
			relationsTbl.delete(deleteFansList);
			
			//3）在微博内容接收邮件箱表，移除该用户的这些被取消关注用户微博内容通知记录
			HTableInterface receiveContentTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_EMAIL));
			Delete deleteRCE = new Delete(Bytes.toBytes(uid));
			for(String attend : attends){
				//deleteColumn 删除最近版本
				//deleteRCE.deleteColumn(Bytes.toBytes("cf"), Bytes.toBytes(attend));
				//deleteColumns 删除单元格所有版本
				long timestamp = System.currentTimeMillis();
				deleteRCE.deleteColumns(Bytes.toBytes("cf"), Bytes.toBytes(attend), timestamp + 100000L);
			}
			receiveContentTbl.delete(deleteRCE);
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(hconn != null){
				try {
					hconn.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 用户获取所关注用户的微博内容
	 * 1）从微博内容邮件箱表，获取用户其关注用户的微博内容rowkey
	 * 2）从微博内容表获取微博内容
	 * @param uid
	 * @return
	 */
	public List<Message> getAttendContens(String uid){
		List<Message> msgList = new ArrayList<Message>();
		HConnection hconn = null;
		try {
			hconn = HConnectionManager.createConnection(conf);
			
			//1）在微博用户关系表，针对该用户，删除被取消的关注用户所对应的单元格
			HTableInterface rceTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_EMAIL));
			Get get = new Get(Bytes.toBytes(uid));
			get.setMaxVersions(5);
			Result result = rceTbl.get(get);
			List<byte[]> rks = new ArrayList<byte[]>();
			Cell[] cells = result.rawCells();
			if(null != cells && cells.length > 0){
				for(Cell cell : cells){
					byte[] rk = CellUtil.cloneRow(cell);
					rks.add(rk);
				}
			}
			
			//2）从微博内容表获取微博内容
			if(rks.size() > 0){
				HTableInterface contentTbl = hconn.getTable(TableName.valueOf(TABLE_WEIBO_CONTENT));
				Scan scan = new Scan();
				List<Get> getList = new ArrayList<Get>();
				for(byte[] rk : rks){
					Get g = new Get(rk);
					getList.add(get);
				}
				Result[] results = contentTbl.get(getList);
				for(Result r : results){
					Cell[] cls = r.rawCells();
					
					for(Cell cell : cls){
						String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
						String attendUid = rowkey.substring(0, rowkey.indexOf("_"));
					}
				}
			}
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(hconn != null){
				try {
					hconn.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return msgList;
	}
	
	public static void main(String[] args) {
		Weibo weibo = new Weibo();
//		weibo.initNamespace();
//		weibo.initTable();
		// put 'weibo:relations', '0001', 'fans:0002', '0002'
		weibo.publishWeiboContent("0001", "今天天气好晴朗");
		// scan 'weibo:content'
		// scan 'weibo:receive-content-email'
		// truncate 'weibo:receive-content-email'
		//scan 'weibo:receive-content-email', {VERSION => 5}
		
//		weibo.publishWeiboContent("0003", "今天天气好晴朗");
//		weibo.publishWeiboContent("0003", "今天天气好晴朗");
//		weibo.publishWeiboContent("0004", "今天天气好晴朗");
//		weibo.publishWeiboContent("0004", "今天天气好晴朗");
//		weibo.publishWeiboContent("0005", "今天天气好晴朗");
//		
//		weibo.addAttends("0001", "0003", "0004", "0005");
//		
//		weibo.removeAttends("0001", "0003");
//		
//		for(int i = 0; i < 1000; i++){
//			weibo.publishWeiboContent("user"+ i, "今天天气好晴朗");
//		}

	}
	
}
