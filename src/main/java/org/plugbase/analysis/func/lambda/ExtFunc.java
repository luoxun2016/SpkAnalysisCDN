package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountExt;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class ExtFunc {
	
	public static Tuple2<String, CountExt> mapToPair(CountDetail detail){
		CountExt countExt = new CountExt(detail);
		
		String domain = detail.getDomain();
		String created = detail.getCreated();
		int mark = detail.getMark();
		String ip = detail.getIp();
		int ext = detail.getExt();
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain.hashCode()).append("-").append(ip).append("-").append(ext).append("-").append(mark).append("-").append(created);
		
		Tuple2<String,CountExt> tuple2 = new Tuple2<String,CountExt>(sb.toString(),countExt);
		
		return tuple2;
	}
	
	public static CountExt reduceByKey(CountExt v1, CountExt v2){
		v1.setFlux(v1.getFlux()+v2.getFlux());
		v1.setIpcount(v1.getIpcount()+v2.getIpcount());
		return v1;
	}

	/**
	 * 	private double flux;	// 下行流量
		private long ip;		// 访问IP
		private int ext;		// 扩展类型1：图片 0：其它
		private int ipcount;	// 次数统计
		private int year; 		// 年
		private int month; 		// 月
		private int day; 		// 日
		private String domain; 	// 域名
		private int mark; 		// 13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
		private int agentid; 	// 代理商ID
		private int userid; 	// 用户ID
		private int serviceid;  // 服务ID
	 * @param row
	 * @return
	 */
	public static Tuple2<String, CountExt> mapToPair(Row row) {
		double flux 		= row.getAs("flux");
		long ip 			= row.getAs("ip");
		int ext 			= row.getAs("ext");
		int ipcount 		= row.getAs("ipcount");
		int year 			= row.getAs("years");
		int month 			= row.getAs("months");
		int day 			= row.getAs("days");
		String domainname 	= row.getAs("domainname");
		int domain			= row.getAs("domain");
		int mark 			= row.getAs("mark");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int serviceid 		= row.getAs("serviceid");
		
		CountExt countExt = new CountExt(flux, ip, ext, ipcount, year, month, day, domainname, domain, mark, agentid, userid, serviceid);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("-").append(day).append("-").append(ip).append("-").append(ext).append("-").append(mark);
		
		Tuple2<String,CountExt> tuple2 = new Tuple2<String,CountExt>(sb.toString(),countExt);
		
		return tuple2;
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountExt>> it) {
		String insertSql = "INSERT INTO `cdn_ext` (`agentid`, `days`,`domainname`,`domain`, `ext`,`flux`,`ip`,`ipcount`,`mark`, `months`, `serviceid`, `userid`,`years`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountExt CountExt = it.next()._2;
			
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			sb.append("('").append(CountExt.getAgentid()).append("'").append(",");
			sb.append("'").append(CountExt.getDays()).append("'").append(",");
			sb.append("'").append(CountExt.getDomainname()).append("'").append(",");
			sb.append("'").append(CountExt.getDomain()).append("'").append(",");
			sb.append("'").append(CountExt.getExt()).append("'").append(",");
			sb.append("'").append(CountExt.getFlux()).append("'").append(",");
			sb.append("'").append(CountExt.getIp()).append("'").append(",");
			sb.append("'").append(CountExt.getIpcount()).append("'").append(",");
			sb.append("'").append(CountExt.getMark()).append("'").append(",");
			sb.append("'").append(CountExt.getMonths()).append("'").append(",");
			sb.append("'").append(CountExt.getServiceid()).append("'").append(",");
			sb.append("'").append(CountExt.getUserid()).append("'").append(",");
			sb.append("'").append(CountExt.getYears()).append("')");
			
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
