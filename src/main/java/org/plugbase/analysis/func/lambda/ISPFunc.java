package org.plugbase.analysis.func.lambda;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountISP;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class ISPFunc {
	
	public static CountISP reduceByKey(CountISP v1, CountISP v2){
		v1.setNumber(v1.getNumber()+v2.getNumber());
		v1.setFlux(v1.getFlux()+v2.getFlux());
		return v1;
	}
	
	public static CountISP reduceByKeyWithIpcount(CountISP v1, CountISP v2){
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
	public static Tuple2<String, CountISP> mapToPairForInfo(Row row){
		int domain 			= row.getAs("domain");
		short name 			= row.getAs("isp");
		long number 		= row.getAs("hitmisscount");
		String created 		= row.getAs("created");
		double flux 		= row.getAs("flux");
		int serviceid 		= row.getAs("serviceid");
		int agentid 		= row.getAs("agentid");
		int userid 			= row.getAs("userid");
		int mark 			= row.getAs("mark");
		long ip 			= row.getAs("ip");
		long ipcount		= 1;
		
		CountISP countISP = new CountISP(domain, name, number, ipcount, flux, created, serviceid, agentid, userid, mark);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("_").append(name).append("_").append(mark).append("_").append(created).append("_").append(ip);
		
		Tuple2<String, CountISP> tuple2 = new Tuple2<String, CountISP>(sb.toString(), countISP);
		
		return tuple2;
	}
	
	/**
		private int domain;		//域名哈希
		private int name;		//运营商
		private int number;		//点击量
		private int ipcount;	//不相同IP数据
		private double flux;	//下行流量
		private int created;	//时间
		private int serviceid;	//服务ID
		private int agentid;	//代理商ID
		private int userid;		//用户ID
		private int mark;		//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
	*/
	public static Tuple2<String, CountISP> mapToPair(Row row){
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
		
		CountISP countISP = new CountISP(domain, name, number, ipcount, flux, created, serviceid, agentid, userid, mark);
		
		StringBuilder sb=new StringBuilder();
		sb.append(domain).append("_").append(name).append("_").append(mark).append("_").append(created).append("_").append(ip);
		
		Tuple2<String, CountISP> tuple2 = new Tuple2<String, CountISP>(sb.toString(), countISP);
		
		return tuple2;
	}
	
	public static Tuple2<String, CountISP> mapToPair(Tuple2<String, CountISP> tuple2){
		CountISP countISP = tuple2._2;
		
		StringBuilder sb=new StringBuilder();
		sb.append(countISP.getDomain()).append("_").append(countISP.getName()).append("_").append(countISP.getMark()).append("_").append(countISP.getCreated());
		
		return new Tuple2<String, CountISP>(sb.toString(), countISP);
	}
	
	public static void foreachPartition(Iterator<Tuple2<String, CountISP>> it) {
		String insertSql = "INSERT INTO `cdn_isp` (`domain`, `name`, `number`, `created`, `ipcount`, `flux`, `serviceid`, `agentid`, `userid`, `mark`) VALUES ";	
		StringBuilder sb = new StringBuilder();
		int index = 0;
		while(it.hasNext()){
			CountISP countISP = it.next()._2;
			
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			
			sb.append("('").append(countISP.getDomain()).append("'").append(",");
			sb.append("'").append(countISP.getName()).append("'").append(",");
			sb.append("'").append(countISP.getNumber()).append("'").append(",");
			sb.append("'").append(countISP.getCreated()).append("'").append(",");
			sb.append("'").append(countISP.getIpcount()).append("'").append(",");
			sb.append("'").append(countISP.getFlux()).append("'").append(",");
			sb.append("'").append(countISP.getServiceid()).append("'").append(",");
			sb.append("'").append(countISP.getAgentid()).append("'").append(",");
			sb.append("'").append(countISP.getUserid()).append("'").append(",");
			sb.append("'").append(countISP.getMark()).append("')");
			
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
