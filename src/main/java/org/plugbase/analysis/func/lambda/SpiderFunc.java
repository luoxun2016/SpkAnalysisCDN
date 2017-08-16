package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountSpider;
import org.plugbase.analysis.util.DBHelper;
import org.plugbase.analysis.util.SpiderUtil;

import scala.Tuple2;

public class SpiderFunc {
	
	public static boolean filter(CountDetail detail){
		int spidertype = detail.getSpidertype(); 
		return spidertype != SpiderUtil.OTHER;
	}
	
	public static Tuple2<String, CountSpider> mapToPair(CountDetail detail){
		int spider = detail.getSpidertype();
		String created = detail.getCreated();
		String domain = detail.getDomain();
		int mark = detail.getMark();
		
		CountSpider countSpider = new CountSpider();
		countSpider.setNumber(1);
		countSpider.setCreated(created);
		countSpider.setType(spider);
		countSpider.setMark(mark);
		countSpider.setDomain(domain.hashCode());
		countSpider.setAgentid(detail.getAgentid());
		countSpider.setUserid(detail.getUserid());
		countSpider.setServiceid(detail.getServiceid());
		
		StringBuilder sb = new StringBuilder();
		sb.append(domain).append("_").append(spider).append("_").append(created).append("_").append(mark);
		
		Tuple2<String, CountSpider> tuple2 = new Tuple2<String, CountSpider>(sb.toString(),countSpider);
		
		return tuple2; 
	}
	
	public static CountSpider reduceByKey(CountSpider v1, CountSpider v2){
		v1.setNumber(v1.getNumber()+v2.getNumber());
		return v1;
	}

	/**
		private int number;			//次数
		private String created ;	//创建时间年-月-日
		private int type ;			//爬虫类型（google，百度）
		private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
		private int domain;			//域名hashcode
		private int agentid;		//代理商ID
		private int userid;			//用户ID
		private int serviceid;		//服务ID
	 * @param row
	 * @return
	 */
	public static Tuple2<String, CountSpider> mapToPair(Row row) {
		int number 		= row.getAs("number");
		String created 	= row.getAs("created");
		int type 		= row.getAs("type");
		int mark 		= row.getAs("mark");
		int domain 		= row.getAs("domain");
		int agentid 	= row.getAs("agentid");
		int userid 		= row.getAs("userid");
		int serviceid 	= row.getAs("serviceid");
		
		CountSpider countSpider = new CountSpider(number, created, type, mark, domain, agentid, userid, serviceid);
		
		StringBuilder sb = new StringBuilder();
		sb.append(domain).append("_").append(type).append("_").append(created).append("_").append(mark);
		
		Tuple2<String, CountSpider> tuple2 = new Tuple2<String, CountSpider>(sb.toString(),countSpider);
		
		return tuple2;
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountSpider>> it) {
		String insertSql = "INSERT INTO `cdn_spider` (`agentid`, `number`, `created`, `domain`, `mark`, `serviceid`,`type`,`userid`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountSpider countSpider = it.next()._2;
		
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			sb.append("('").append(countSpider.getAgentid()).append("'").append(",");
			sb.append("'").append(countSpider.getNumber()).append("'").append(",");
			sb.append("'").append(countSpider.getCreated()).append("'").append(",");
			sb.append("'").append(countSpider.getDomain()).append("'").append(",");
			sb.append("'").append(countSpider.getMark()).append("'").append(",");
			sb.append("'").append(countSpider.getServiceid()).append("'").append(",");
			sb.append("'").append(countSpider.getType()).append("'").append(",");
			sb.append("'").append(countSpider.getUserid()).append("')");
			
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
