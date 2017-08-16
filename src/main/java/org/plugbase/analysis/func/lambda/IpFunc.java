package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountIP;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class IpFunc {
	
	public static Tuple2<String, CountIP> mapToPair(CountDetail detail){
		CountIP countIP = new CountIP(detail);
		
		String domain = detail.getDomain();
		int day=detail.getDay();
		int mark=detail.getMark();
		String ip=detail.getIp();
		
		StringBuilder sb = new StringBuilder();
		sb.append(domain.hashCode()).append("-").append(ip).append("-").append(day).append("_").append(mark);
		
		Tuple2<String,CountIP> tuple2 = new Tuple2<String,CountIP>(sb.toString(),countIP);
		
		return tuple2;
	}
	
	public static CountIP reduceByKey(CountIP v1, CountIP v2){
		v1.setIpcount(v1.getIpcount()+v2.getIpcount());
		v1.setFlux(v1.getFlux()+ v2.getFlux());
		return v1;
	}

	/**
	 * 	private int number;		// 次数统计
		private double flux; 	// 下行流量
		private long ip; 		// 访问IP to Long
		private int filenum; 	// 文件请求次数统计
		private int year; 		// 年
		private int month; 		// 月
		private int day; 		// 日
		private String domain; 	// 域名
		private int mark; 		// 13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
		private int agentid; 	// 代理商ID
		private int userid; 	// 用户ID
		private int serviceid; 	// 服务ID
	 * @param row
	 * @return
	 */
	public static Tuple2<String, CountIP> mapToPair(Row row) {
		int ipcount 	= row.getAs("ipcount");
		double flux 	= row.getAs("flux");
		long ip 		= row.getAs("ip");
		String created 	= row.getAs("created");
		int domain 		= row.getAs("domain");
		int mark 		= row.getAs("mark");
		int agentid 	= row.getAs("agentid");
		int userid 		= row.getAs("userid");
		int serviceid 	= row.getAs("serviceid");
		
		CountIP countIP = new CountIP(ipcount, flux, ip, created, domain, mark, agentid, userid, serviceid);
		
		StringBuilder sb = new StringBuilder();
		sb.append(domain).append("-").append(ip).append("-").append(created).append("_").append(mark);
		
		Tuple2<String,CountIP> tuple2 = new Tuple2<String,CountIP>(sb.toString(),countIP);
		
		return tuple2;
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountIP>> it) {
		String insertSql = "INSERT INTO `cdn_ip` (`agentid`,`domain`,`flux`,`ip`,`mark`, `ipcount`,`serviceid`,`userid`,`created`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountIP countIP = it.next()._2;
			
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			sb.append("('").append(countIP.getAgentid()).append("'").append(",");
			sb.append("'").append(countIP.getDomain()).append("'").append(",");
			sb.append("'").append(countIP.getFlux()).append("'").append(",");
			sb.append("'").append(countIP.getIp()).append("'").append(",");
			sb.append("'").append(countIP.getMark()).append("'").append(",");
			sb.append("'").append(countIP.getIpcount()).append("'").append(",");
			sb.append("'").append(countIP.getServiceid()).append("'").append(",");
			sb.append("'").append(countIP.getUserid()).append("'").append(",");
			sb.append("'").append(countIP.getCreated()).append("')");
			
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
