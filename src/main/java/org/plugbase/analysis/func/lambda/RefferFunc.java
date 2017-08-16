package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountReffer;
import org.plugbase.analysis.util.DBHelper;
import org.plugbase.analysis.util.DomainCompare;

import scala.Tuple2;

public class RefferFunc {
	
	public static boolean filter(CountDetail detail){
		String refferTemp = detail.getReffer().trim();
		if(!refferTemp.isEmpty()){
			String domain = detail.getDomain();
			if (!refferTemp.contains(domain)) {
				if (!DomainCompare.levelDomain(refferTemp).equals(DomainCompare.levelDomain(domain))) {
					return true;
				}
			}
		}
		return false;
	}
	
	public static Tuple2<String,CountReffer> mapToPair(CountDetail detail){
		String reffer = detail.getReffer().trim();
		String domain = detail.getDomain();
		int years=detail.getYear();
		int months=detail.getMonth();
		int days=detail.getDay();
		int mark=detail.getMark();
		String date = String.format("%1$s-%2$02d-%3$02d", years,months,days);
		
		CountReffer countReffer = new CountReffer(detail,reffer,date);
		
		StringBuilder sb = new StringBuilder();
		sb.append(domain).append("_").append(reffer).append("_").append(mark);
		
		Tuple2<String,CountReffer> tuple2 = new Tuple2<String,CountReffer>(sb.toString(),countReffer);
		
		return tuple2;
	}
	
	public static CountReffer reduceByKey(CountReffer v1, CountReffer v2){
		v1.setNumber(v1.getNumber()+v2.getNumber());
		return v1;
	}

	/**
		private String created;	//时间年-月-日
		private long number;	//次数
		private String reffer;	//来源
		private int domain;		//域名
		private int mark;		//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
		private int agentid;	//代理商ID
		private int userid;		//用户ID
		private int serviceid;	//服务ID
	 * @param row
	 * @return
	 */
	public static Tuple2<String,CountReffer> mapToPair(Row row) {
		String created 	= row.getAs("created");
		long number 	= row.getAs("number");
		String reffer 	= row.getAs("reffer");
		int domain 		= row.getAs("domain");
		int mark 		= row.getAs("mark");
		int agentid 	= row.getAs("agentid");
		int userid 		= row.getAs("userid");
		int serviceid 	= row.getAs("serviceid");
		
		CountReffer countReffer = new CountReffer(created, number, reffer, domain, mark, agentid, userid, serviceid);
		
		StringBuilder sb = new StringBuilder();
		sb.append(domain).append("_").append(reffer).append("_").append(mark).append("_").append(created);
		
		Tuple2<String,CountReffer> tuple2 = new Tuple2<String,CountReffer>(sb.toString(),countReffer);
		
		return tuple2;
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountReffer>> it) {
		String insertSql = "INSERT INTO `cdn_reffer` (`agentid`, `domain`, `mark`, `number`, `reffer`, `serviceid`, `created`, `userid`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountReffer countReffer = it.next()._2;
		    
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			sb.append("('").append(countReffer.getAgentid()).append("'").append(",");
			sb.append("'").append(countReffer.getDomain()).append("'").append(",");
			sb.append("'").append(countReffer.getMark()).append("'").append(",");
			sb.append("'").append(countReffer.getNumber()).append("'").append(",");
			sb.append("'").append(countReffer.getReffer()).append("'").append(",");
			sb.append("'").append(countReffer.getServiceid()).append("'").append(",");
			sb.append("'").append(countReffer.getCreated()).append("'").append(",");
			sb.append("'").append(countReffer.getUserid()).append("')");
			
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
