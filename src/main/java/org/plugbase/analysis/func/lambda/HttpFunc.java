package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountHttp;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class HttpFunc {
	
	public static Tuple2<String, CountHttp> mapToPair(CountDetail detail) {
		CountHttp countHttp = new CountHttp(detail);
		
		String created = detail.getCreated();
		int mark = detail.getMark();

		StringBuilder sb = new StringBuilder();
		sb.append(countHttp.getDomain()).append("-").append(created).append("-").append(mark);
		
		Tuple2<String, CountHttp> tuple2 = new Tuple2<String, CountHttp>(sb.toString(), countHttp);
		
		return tuple2;
	}

	public static CountHttp reduceByKey(CountHttp v1, CountHttp v2) {
		v1.setHttprequest(v1.getHttprequest() + v2.getHttprequest());
		v1.setFilecount(v1.getFilecount() + v2.getFilecount());
		v1.setPagecount(v1.getPagecount() + v2.getPagecount());
		v1.setFlux(v1.getFlux() + v2.getFlux());
		return v1;
	}

	/**
		private double flux;		//下行流量
		private long httprequest;	//次数统计
		private int filecount;		//文件请求次数
		private int pagecount;		//页面请求次数
		private int year;			//年
		private int month;			//月
		private int day;			//日
		private int hour;			//时
		private String domainname;	//域名
		private int domain;			//域名hashcode
		private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
		private int agentid;		//代理商ID
		private int userid;			//用户ID
		private int serviceid;		//服务ID
	 * @param row
	 * @return
	 */
	public static Tuple2<String, CountHttp> mapToPair(Row row) {
		double flux 		= row.getAs("flux");
		long number 		= row.getAs("httprequest");
		int filenum 		= row.getAs("filecount");
		int webnum 			= row.getAs("pagecount");
		int year 			= row.getAs("year");
		int month 			= row.getAs("month");
		int day 			= row.getAs("day");
		int hour 			= row.getAs("hour");
		String domainname	= row.getAs("domainname");
		int domain			= row.getAs("domain");
		int mark 			= row.getAs("mark");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int serviceid 		= row.getAs("serviceid");
		
		CountHttp countHttp = new CountHttp(flux, number, filenum, webnum, year, month, day, hour, domainname, domain, mark, agentid, userid, serviceid);
		
		StringBuilder sb = new StringBuilder();
		sb.append(domain).append("-").append(year).append("-").append(month).append("-").append(day).append("-").append(mark);
		
		Tuple2<String, CountHttp> tuple2 = new Tuple2<String, CountHttp>(sb.toString(), countHttp);
		
		return tuple2;
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountHttp>> it) {
		String insertSql = "INSERT INTO `cdn_http` (`agentid`,`day`,`domainname`,`domain`,`filecount`,`flux`,`hour`,`mark`,`month`,`httprequest`,`serviceid`,`userid`,`pagecount`,`year`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountHttp countHttp = it.next()._2;
		    
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			sb.append("('").append(countHttp.getAgentid()).append("'").append(",");
			sb.append("'").append(countHttp.getDay()).append("'").append(",");
			sb.append("'").append(countHttp.getDomainname()).append("'").append(",");
			sb.append("'").append(countHttp.getDomain()).append("'").append(",");
			sb.append("'").append(countHttp.getFilecount()).append("'").append(",");
			sb.append("'").append(countHttp.getFlux()).append("'").append(",");
			sb.append("'").append(countHttp.getHour()).append("'").append(",");
			sb.append("'").append(countHttp.getMark()).append("'").append(",");
			sb.append("'").append(countHttp.getMonth()).append("'").append(",");
			sb.append("'").append(countHttp.getHttprequest()).append("'").append(",");
			sb.append("'").append(countHttp.getServiceid()).append("'").append(",");
			sb.append("'").append(countHttp.getUserid()).append("'").append(",");
			sb.append("'").append(countHttp.getPagecount()).append("'").append(",");
			sb.append("'").append(countHttp.getYear()).append("')");
			
			if(++index >= 1000){
				String sql = sb.toString();
				try {
					DBHelper.getInstance().executeUpdate(sql);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				sb.delete(0, sb.length());
				index = 0;
			}
		}
		if(sb.length() != 0){
			String sql = sb.toString();
			try {
				DBHelper.getInstance().executeUpdate(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
}
