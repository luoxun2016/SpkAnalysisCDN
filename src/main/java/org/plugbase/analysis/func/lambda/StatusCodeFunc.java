package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountStatusCode;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class StatusCodeFunc {
	
	public static CountStatusCode reduceByKey(CountStatusCode v1, CountStatusCode v2){
		v1.setNumber(v1.getNumber()+v2.getNumber());
		v1.setFlux(v1.getFlux()+v2.getFlux());
		return v1;
	}
	
	public static CountStatusCode reduceByKeyWithIpcount(CountStatusCode v1, CountStatusCode v2){
		v1.setNumber(v1.getNumber()+v2.getNumber());
		v1.setFlux(v1.getFlux()+v2.getFlux());
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
	public static Tuple2<String, CountStatusCode> mapToPairForInfo(Row row){
		int domain 			= row.getAs("domain");
		int name 			= row.getAs("status");
		long number 		= row.getAs("hitmisscount");
		String created 		= row.getAs("created");
		double flux 		= row.getAs("flux");
		int serviceid 		= row.getAs("serviceid");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int mark 			= row.getAs("mark");
		long ip 			= row.getAs("ip");
		long ipcount		= 1;
		
		CountStatusCode countStatusCode = new CountStatusCode(domain, name, number, flux, ipcount, created, serviceid, agentid, userid, mark);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("_").append(name).append("_").append(mark).append("_").append(created).append("_").append(ip);
		
		Tuple2<String, CountStatusCode> tuple2 = new Tuple2<String, CountStatusCode>(sb.toString(), countStatusCode);
		
		return tuple2;
	}
	
	/**
		private int domain;		//域名哈希
		private short name;		//状态码
		private long number;	//请求次数
		private double flux;	//下行流量
		private long ipcount;	//有多少个不相同的IP
		private String created;	//时间
		private int serviceid;	//服务ID
		private int agentid;	//代理商ID
		private int userid;		//用户ID
		private int mark;		//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
	*/
	public static Tuple2<String, CountStatusCode> mapToPair(Row row){
		int domain 			= row.getAs("domain");
		short name 			= row.getAs("name");
		long number 		= row.getAs("number");
		String created 		= row.getAs("created");
		double flux 		= row.getAs("flux");
		int serviceid 		= row.getAs("serviceid");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int mark 			= row.getAs("mark");
		long ip 			= row.getAs("ip");
		long ipcount		= row.getAs("ipcount");
		
		CountStatusCode countStatusCode = new CountStatusCode(domain, name, number, flux, ipcount, created, serviceid, agentid, userid, mark);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("_").append(name).append("_").append(mark).append("_").append(created).append("_").append(ip);
		
		Tuple2<String, CountStatusCode> tuple2 = new Tuple2<String, CountStatusCode>(sb.toString(), countStatusCode);
		
		return tuple2;
	}
	
	public static Tuple2<String, CountStatusCode> mapToPair(Tuple2<String, CountStatusCode> tuple2){
		CountStatusCode countStatusCode = tuple2._2;
		
		StringBuilder sb=new StringBuilder();
		sb.append(countStatusCode.getDomain()).append("_").append(countStatusCode.getName()).append("_").append(countStatusCode.getMark()).append("_").append(countStatusCode.getCreated());
		
		return new Tuple2<String, CountStatusCode>(sb.toString(), countStatusCode);
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountStatusCode>> it) {
		String insertSql = "INSERT INTO `cdn_statuscode` (`domain`, `name`, `number`, `created`, `ipcount`, `flux`, `serviceid`, `agentid`, `userid`, `mark`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountStatusCode countStatusCode = it.next()._2;
			
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			
			sb.append("('").append(countStatusCode.getDomain()).append("'").append(",");
			sb.append("'").append(countStatusCode.getName()).append("'").append(",");
			sb.append("'").append(countStatusCode.getNumber()).append("'").append(",");
			sb.append("'").append(countStatusCode.getCreated()).append("'").append(",");
			sb.append("'").append(countStatusCode.getIpcount()).append("'").append(",");
			sb.append("'").append(countStatusCode.getFlux()).append("'").append(",");
			sb.append("'").append(countStatusCode.getServiceid()).append("'").append(",");
			sb.append("'").append(countStatusCode.getAgentid()).append("'").append(",");
			sb.append("'").append(countStatusCode.getUserid()).append("'").append(",");
			sb.append("'").append(countStatusCode.getMark()).append("')");
			
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
