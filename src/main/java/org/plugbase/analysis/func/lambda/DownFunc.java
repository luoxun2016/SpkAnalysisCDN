package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountDown;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class DownFunc {
	private static final int _1M = 1024*1024;

	public static boolean filter(CountDetail detail){
		if(detail.getBandwidth() > _1M){
			return true;
		}
		return false;
	}
	
	public static Tuple2<String, CountDown> mapToPair(CountDetail detail){
		CountDown countFileDown = new CountDown(detail);
		
		String created 	= detail.getCreated();
		int mark 		= detail.getMark();
		
		StringBuilder sb = new StringBuilder();
		sb.append(countFileDown.getDomain()).append("-").append(countFileDown.getFileurlhash()).append("-").append(created).append("-").append(mark);
		
		Tuple2<String, CountDown> tuple2 = new Tuple2<String, CountDown>(sb.toString(), countFileDown);
		
		return tuple2;
	}
	
	public static CountDown reduceByKey(CountDown v1, CountDown v2){
		v1.setTotalcount(v1.getTotalcount()+v2.getTotalcount());
		v1.setFinishedcount(v1.getFinishedcount()+v2.getFinishedcount());
		v1.setConvercount(v1.getConvercount()+v2.getConvercount());
		return v1;
	}

	/**
		private String domainname;	//域名
		private int domain;			//域名哈希
		private String fileurl;		//访问URL
		private int fileurlhash;	//访问URL哈希
		private double size;		//文件大小
		private double totalcount;	//总下载大小
		private int finishedcount;	//成功下载次数 且 是非站内下载
		private int convercount;	//总下载次数
		private String created;		//时间年月日
		private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
		private int agentid;		//代理商ID
		private int userid;			//用户ID
		private int serviceid;		//服务ID
	 * @param row
	 * @return
	 */
	public static Tuple2<String, CountDown> mapToPair(Row row) {
		String created 		= row.getAs("created");
		String fileurl 		= row.getAs("fileurl");
		int fileurlhash 	= row.getAs("fileurlhash");
		double size 		= row.getAs("size");
		int finishedcount 	= row.getAs("finishedcount");
		int convercount 	= row.getAs("convercount");
		double totalcount 	= row.getAs("totalcount");
		String domainname 	= row.getAs("domainname");
		int domain 			= row.getAs("domain");
		int mark 			= row.getAs("mark");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int serviceid 		= row.getAs("serviceid");
		
		CountDown countFileDown = new CountDown(created, fileurl, fileurlhash, size, finishedcount, convercount, totalcount, domainname, domain, mark, agentid, userid, serviceid);
		
		StringBuilder sb = new StringBuilder();
		sb.append(domain).append("-").append(fileurlhash).append("-").append(created).append("-").append(mark);
		
		Tuple2<String, CountDown> tuple2 = new Tuple2<String, CountDown>(sb.toString(), countFileDown);
		
		return tuple2;
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountDown>> it) {
		String insertSql = "INSERT INTO `cdn_download` (`domainname`,`domain`,`fileurl`,`fileurlhash`,`size`,`totalcount`,`finishedcount`,`convercount`,`created`,`serviceid`,`agentid`,`userid`,`mark`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountDown countDown = it.next()._2;
			
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			sb.append("('").append(countDown.getDomainname()).append("'").append(",");
			sb.append("'").append(countDown.getDomain()).append("'").append(",");
			sb.append("'").append(countDown.getFileurl()).append("'").append(",");
			sb.append("'").append(countDown.getFileurlhash()).append("'").append(",");
			sb.append("'").append(countDown.getSize()).append("'").append(",");
			sb.append("'").append(countDown.getTotalcount()).append("'").append(",");
			sb.append("'").append(countDown.getFinishedcount()).append("'").append(",");
			sb.append("'").append(countDown.getConvercount()).append("'").append(",");
			sb.append("'").append(countDown.getCreated()).append("'").append(",");
			sb.append("'").append(countDown.getServiceid()).append("'").append(",");
			sb.append("'").append(countDown.getAgentid()).append("'").append(",");
			sb.append("'").append(countDown.getUserid()).append("'").append(",");
			sb.append("'").append(countDown.getMark()).append("')");
			
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
