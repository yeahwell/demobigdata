package com.webmovie.bigdata.storm.action01.hbase.dao;

import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public interface HBaseDao {

	/**
	 * 保存数据
	 * 
	 * @param put
	 * @param tableName
	 */
	public void save(Put put, String tableName);

	/**
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quailifer
	 * @param value
	 */
	public void insert(String tableName, String rowKey, String family,
			String quailifer, String value);

	/**
	 * 批量保存数据
	 * 
	 * @param Put
	 * @param tableName
	 */
	public void save(List<Put> Put, String tableName);

	/**
	 * 查询单行
	 * 
	 * @param tableName
	 * @param rowKey
	 * @return
	 */
	public Result getOneRow(String tableName, String rowKey);

	/**
	 * 查询多行
	 * 
	 * @param tableName
	 * @param rowKey_like
	 * @return
	 */
	public List<Result> getRows(String tableName, String rowKey_like);

	public List<Result> getRows(String tableName, String rowKeyLike,
			String cols[]);

	public List<Result> getRows(String tableName, String startRow,
			String stopRow);

}
