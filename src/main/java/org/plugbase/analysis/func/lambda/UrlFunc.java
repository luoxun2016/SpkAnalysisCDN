package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountUrl;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class UrlFunc {
	
	public static Tuple2<String, CountUrl> mapToPair(CountDetail detail){
		CountUrl countUrl=new CountUrl(detail);
		
		String domain = detail.getDomain();
		String created = detail.getCreated();
		int mark= detail.getMark();
		String url= detail.getUrl();
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain.hashCode()).append("-").append(url.hashCode()).append("-").append(created).append("-").append(mark);
		
		Tuple2<String,CountUrl> tuple2 = new Tuple2<String,CountUrl>(sb.toString(),countUrl);
		
		return tuple2;
	}
	
	public static CountUrl reduceByKey(CountUrl v1, CountUrl v2){
		v1.setVisitcount(v1.getVisitcount()+v2.getVisitcount());			
		v1.setInbound(v1.getInbound()+v2.getInbound());
		return v1;
	}

	/**
	 * 	private String url;			//访问url
		private long number;		//次数统计
		private int count;			//非站内访问次数
		private int year;			//年
		private int month;			//月
		private int day;			//日
		private String domain;		//域名
		private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
		private int agentid;		//代理商ID
		private int userid;			//用户ID
		private int serviceid;		//服务ID
	 * @param row
	 * @return
	 */
	public static Tuple2<String, CountUrl> mapToPair(Row row) {
		String domainname 	= row.getAs("domainname");
		int domain 			= row.getAs("domain");
		String url 			= row.getAs("url");
		int urlh			= row.getAs("urlh");
		long visitcount 	= row.getAs("visitcount");
		int inbound 		= row.getAs("inbound");
		String created 		= row.getAs("created");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int serviceid 		= row.getAs("serviceid");
		int mark 			= row.getAs("mark");
		
		CountUrl countUrl = new CountUrl(url, urlh, visitcount, inbound, created, domainname, domain, agentid, userid, serviceid, mark);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("-").append(url.hashCode()).append("-").append(created);
		
		Tuple2<String,CountUrl> tuple2 = new Tuple2<String,CountUrl>(sb.toString(),countUrl);
		
		return tuple2;
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountUrl>> it) {
		String insertSql = "INSERT INTO `cdn_url` (`domainname`,`domain`,`url`,`urlh`,`visitcount`,`inbound`,`created`,`agentid`,`userid`,`serviceid`,`mark`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountUrl countUrl = it.next()._2;
		    
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			sb.append("('").append(countUrl.getDomainname()).append("'").append(",");
			sb.append("'").append(countUrl.getDomain()).append("'").append(",");
			sb.append("'").append(countUrl.getUrl()).append("'").append(",");
			sb.append("'").append(countUrl.getUrlh()).append("'").append(",");
			sb.append("'").append(countUrl.getVisitcount()).append("'").append(",");
			sb.append("'").append(countUrl.getInbound()).append("'").append(",");
			sb.append("'").append(countUrl.getCreated()).append("'").append(",");
			sb.append("'").append(countUrl.getAgentid()).append("'").append(",");
			sb.append("'").append(countUrl.getUserid()).append("'").append(",");
			sb.append("'").append(countUrl.getServiceid()).append("'").append(",");
			sb.append("'").append(countUrl.getMark()).append("')");
			
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
