package org.plugbase.analysis.func.lambda;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountInfo;

import scala.Tuple2;

public class InfoFunc {
	
	public static Tuple2<String, CountInfo> mapToPair(CountDetail detail){
		CountInfo countInfo = new CountInfo(detail);
		
		int state 	= detail.getState();
		int hitmiss = detail.getHitmiss();
		int mark 	= detail.getMark();
		
		StringBuilder sb=new StringBuilder();
		sb.append(countInfo.getIp()).append("_").append(countInfo.getDomain()).append("_").append(state).append("_").append(hitmiss).append("_").append(mark);
		
		Tuple2<String, CountInfo> tuple2 = new Tuple2<String, CountInfo>(sb.toString(), countInfo);
		
		return tuple2;
	}
	
	public static CountInfo reduceByKey(CountInfo v1, CountInfo v2){
		v1.setFlux(v1.getFlux()+v2.getFlux());
		v1.setHitmisscount(v1.getHitmisscount() + v2.getHitmisscount());
		return v1;
	}

	/**
		private String created;		//创建时间
		private int domain;			//域名hashcode
		private int area;			//区域
		private short isp;			//运营商
		private long ip;			//访问IP to Long
		private long hitmisscount;	//访问次数
		private int status;			//HTTP状态
		private double flux;		//下行流量
		private short hitmiss;		//是否命中：1：命中 0：未命中
		private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
		private int agentid;		//代理商ID
		private int userid;			//用户ID
		private int serviceid;		//服务ID
	 */
	public static Tuple2<String, CountInfo> mapToPair(Row row){
		String created 		= row.getAs("created");
		int domain 			= row.getAs("domain");
		int area 			= row.getAs("area");
		short isp 			= row.getAs("isp");
		long ip 			= row.getAs("ip");
		long hitmisscount 	= row.getAs("hitmisscount");
		int status 			= row.getAs("status");
		double flux 		= row.getAs("flux");
		short hitmiss 		= row.getAs("hitmiss");
		int mark 			= row.getAs("mark");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int serviceid 		= row.getAs("serviceid");
		
		CountInfo countInfo = new CountInfo(created, domain, area, isp, ip, hitmisscount, status, flux, hitmiss, mark, agentid, userid, serviceid);
		
		StringBuilder sb=new StringBuilder();
		sb.append(countInfo.getIp()).append("_").append(countInfo.getDomain()).append("_").append(status).append("_").append(hitmiss).append("_").append(mark);
		
		Tuple2<String, CountInfo> tuple2 = new Tuple2<String, CountInfo>(sb.toString(), countInfo);
		
		return tuple2;
	}
	
	public static CountInfo parse(Row row){
		String created 		= row.getAs("created");
		int domain 			= row.getAs("domain");
		int area 			= row.getAs("area");
		short isp 			= row.getAs("isp");
		long ip 			= row.getAs("ip");
		long hitmisscount 	= row.getAs("hitmisscount");
		int status 			= row.getAs("status");
		double flux 		= row.getAs("flux");
		short hitmiss 		= row.getAs("hitmiss");
		int mark 			= row.getAs("mark");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int serviceid 		= row.getAs("serviceid");
		
		CountInfo countInfo = new CountInfo(created, domain, area, isp, ip, hitmisscount, status, flux, hitmiss, mark, agentid, userid, serviceid);
		
		return countInfo;
	}
}
