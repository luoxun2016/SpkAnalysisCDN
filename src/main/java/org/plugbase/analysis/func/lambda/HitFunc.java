package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountHit;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class HitFunc {
	
	public static CountHit reduceByKey(CountHit v1, CountHit v2){
		v1.setHitcount(v1.getHitcount()+v2.getHitcount());
		v1.setHitallcount(v1.getHitallcount()+v2.getHitallcount());
		v1.setFlux(v1.getFlux()+v2.getFlux());
		v1.setFluxall(v1.getFluxall()+v2.getFluxall());
		return v1;
	}
	
	public static Tuple2<String, CountHit> mapToPair(CountDetail detail){
		CountHit countHit = new CountHit(detail);
		
		String created = detail.getCreated();
		int mark = detail.getMark();
		
		StringBuilder sb=new StringBuilder();
		sb.append(countHit.getDomain()).append("_").append(mark).append("_").append(created);
		
		Tuple2<String, CountHit> tuple2 = new Tuple2<String, CountHit>(sb.toString(), countHit);
		
		return tuple2;
	}

	/**
		private int domain;			//域名
		private String created;		//创建日期
		private long hitcount;		//命中次数
		private long hitallcount;	//总点击数
		private double flux;		//命中流量
		private double fluxall;		//总流量
		private int serviceid;		//服务ID
		private int agentid;		//代理商ID
		private int userid;			//用户ID
		private int mark;			//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
	 */
	public static Tuple2<String, CountHit> mapToPair(Row row){
		int domain 			= row.getAs("domain");
		String created 		= row.getAs("created");
		long hitcount 		= row.getAs("hitcount");
		long hitallcount 	= row.getAs("hitallcount");
		double flux 		= row.getAs("flux");
		double fluxall 		= row.getAs("fluxall");
		int serviceid 		= row.getAs("serviceid");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int mark 			= row.getAs("mark");
		
		CountHit countHit = new CountHit(domain, created, hitcount, hitallcount, flux, fluxall, serviceid, agentid, userid, mark);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("_").append(mark).append("_").append(created);
		
		Tuple2<String, CountHit> tuple2 = new Tuple2<String, CountHit>(sb.toString(), countHit);
		
		return tuple2;
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountHit>> it) {
		String insertSql = "INSERT INTO `cdn_hit` (`domain`, `created`, `hitcount`, `hitallcount`, `flux`, `fluxall`, `serviceid`, `agentid`, `userid`, `mark`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountHit countHit = it.next()._2;
			
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			
			sb.append("('").append(countHit.getDomain()).append("'").append(",");
			sb.append("'").append(countHit.getCreated()).append("'").append(",");
			sb.append("'").append(countHit.getHitcount()).append("'").append(",");
			sb.append("'").append(countHit.getHitallcount()).append("'").append(",");
			sb.append("'").append(countHit.getFlux()).append("'").append(",");
			sb.append("'").append(countHit.getFluxall()).append("'").append(",");
			sb.append("'").append(countHit.getServiceid()).append("'").append(",");
			sb.append("'").append(countHit.getAgentid()).append("'").append(",");
			sb.append("'").append(countHit.getUserid()).append("'").append(",");
			sb.append("'").append(countHit.getMark()).append("')");
			
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
