package com.webmovie.bigdata.springboot.web;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import backtype.storm.utils.Utils;

import com.webmovie.bigdata.storm.action01.hbase.dao.HBaseDao;
import com.webmovie.bigdata.storm.action01.hbase.dao.impl.HBaseDaoImpl;
import com.webmovie.bigdata.storm.action01.kafka.DateFmt;
import com.webmovie.bigdata.storm.action01.vo.AreaVo;


public class AreaSvlt extends HttpServlet {

	HBaseDao dao = null;

	String today = null;
	
	public void init() throws ServletException {
		dao = new HBaseDaoImpl() ;
		today = DateFmt.getCountDate(null, DateFmt.date_short) ;
		
		
	}

	public void destroy() {
		super.destroy(); // Just puts "destroy" string in log
		// Put your code here
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		this.doPost(request, response) ;
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		
		while(true)
		{
			//每个3s查询一次hbase
			String data = this.getData(today, dao);
			//todayData:123,hisData:456
			String jsDataString = "{\'todayData\':"+data+"}";
			this.sentData("jsFun", response, jsDataString);
			
			
			Utils.sleep(3000) ;
		}
	}
	
	public void sentData(String jsFun,HttpServletResponse response,String data)
	{
		try {
			response.setContentType("text/html;charset=utf-8");
			response.getWriter().write("<script type=\"text/javascript\">parent." + jsFun + "(\""+data+"\")</script>");
			response.flushBuffer() ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public String getData(String date, HBaseDao dao)
	{
		List<Result> list =  dao.getRows("area_order", date) ;
		AreaVo vo = new AreaVo() ;
		for(Result rs : list)
		{
			String rowKey = new String(rs.getRow()) ;
			String aredid = null;
			if (rowKey.split("_").length == 2) {
				aredid = rowKey.split("_")[1] ;
			}
			for(KeyValue keyValue : rs.raw())
			{
				if("order_amt".equals(new String(keyValue.getQualifier())))
				{
					vo.setData(aredid, new String(keyValue.getValue())) ;
					break;
				}
			}
		}
		String result = "["+getFmtPoint(vo.getBeijing())+","+getFmtPoint(vo.getShanghai())+","+getFmtPoint(vo.getGuangzhou())+","+getFmtPoint(vo.getShenzhen())+","+getFmtPoint(vo.getChengdu())+"]" ;
		return result;
		
	}
	public String getFmtPoint(String str)
	{
		DecimalFormat format = new DecimalFormat("#");
		if (str != null) {
			return format.format(Double.parseDouble(str)) ;
		}
		return null;
	}
	

}
