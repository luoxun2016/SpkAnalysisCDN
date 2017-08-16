package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountArea;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class AreaFunc {
	
	public static CountArea reduceByKey(CountArea v1, CountArea v2){
		v1.setNumber(v1.getNumber()+v2.getNumber());
		v1.setBandwidth(v1.getBandwidth()+v2.getBandwidth());
		return v1;
	}
	
	public static CountArea reduceByKeyWithIpcount(CountArea v1, CountArea v2){
		v1.setNumber(v1.getNumber()+v2.getNumber());
		v1.setBandwidth(v1.getBandwidth()+v2.getBandwidth());
		v1.setIpcount(v1.getIpcount()+v2.getIpcount());
		return v1;
	}

	/**
	 * 	private String created;		//创建时间
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
	 * @param row
	 * @return
	 */
	public static Tuple2<String, CountArea> mapToPairForInfo(Row row){
		int domain 			= row.getAs("domain");
		int name 			= row.getAs("area");
		long number 		= row.getAs("hitmisscount");
		String created 		= row.getAs("created");
		double bandwidth 	= row.getAs("flux");
		int serviceid 		= row.getAs("serviceid");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int mark 			= row.getAs("mark");
		long ip 			= row.getAs("ip");
		long ipcount		= 1;
		
		CountArea countArea = new CountArea(domain, name, number, created, ipcount, bandwidth, serviceid, agentid, userid, mark);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("_").append(name).append("_").append(mark).append("_").append(created).append("_").append(ip);
		
		Tuple2<String, CountArea> tuple2 = new Tuple2<String, CountArea>(sb.toString(), countArea);
		
		return tuple2;
	}
	
	/**
		private int domain;			//域名哈希
		private int name;			//区域码
		private int number;			//点击量
		private String created;		//创建时间
		private int ipcount;		//多少个不相同IP
		private double bandwidth;	//下行流量
		private int serviceid;		//服务ID
		private int agentid;		//代理商ID
		private int userid;			//用户ID
		private int mark;			//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
	*/
	public static Tuple2<String, CountArea> mapToPair(Row row){
		int domain 			= row.getAs("domain");
		int name 			= row.getAs("name");
		long number 		= row.getAs("number");
		String created 		= row.getAs("created");
		double bandwidth 	= row.getAs("bandwidth");
		int serviceid 		= row.getAs("serviceid");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int mark 			= row.getAs("mark");
		long ip 			= row.getAs("ip");
		long ipcount		= row.getAs("ipcount");
		
		CountArea countArea = new CountArea(domain, name, number, created, ipcount, bandwidth, serviceid, agentid, userid, mark);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("_").append(name).append("_").append(mark).append("_").append(created).append("_").append(ip);
		
		Tuple2<String, CountArea> tuple2 = new Tuple2<String, CountArea>(sb.toString(), countArea);
		
		return tuple2;
	}
	
	public static Tuple2<String, CountArea> mapToPair(Tuple2<String, CountArea> tuple2){
		CountArea countArea = tuple2._2;
		
		StringBuilder sb=new StringBuilder();
		sb.append(countArea.getDomain()).append("_").append(countArea.getName()).append("_").append(countArea.getMark()).append("_").append(countArea.getCreated());
		
		return new Tuple2<String, CountArea>(sb.toString(), countArea);
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountArea>> it) {
		String insertSql = "INSERT INTO `cdn_area` (`domain`, `name`, `number`, `created`, `ipcount`, `bandwidth`, `serviceid`, `agentid`, `userid`, `mark`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountArea countArea = it.next()._2;
			
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			
			sb.append("('").append(countArea.getDomain()).append("'").append(",");
			sb.append("'").append(countArea.getName()).append("'").append(",");
			sb.append("'").append(countArea.getNumber()).append("'").append(",");
			sb.append("'").append(countArea.getCreated()).append("'").append(",");
			sb.append("'").append(countArea.getIpcount()).append("'").append(",");
			sb.append("'").append(countArea.getBandwidth()).append("'").append(",");
			sb.append("'").append(countArea.getServiceid()).append("'").append(",");
			sb.append("'").append(countArea.getAgentid()).append("'").append(",");
			sb.append("'").append(countArea.getUserid()).append("'").append(",");
			sb.append("'").append(countArea.getMark()).append("')");
			
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
