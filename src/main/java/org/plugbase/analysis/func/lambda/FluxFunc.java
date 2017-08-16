package org.plugbase.analysis.func.lambda;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountFlux;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class FluxFunc {
	private static final Logger log = Logger.getLogger(FluxFunc.class);
	
	public static Tuple2<String, CountFlux> mapToPair(CountDetail detail){
		CountFlux countFlux = new CountFlux(detail);
		if(detail.getHitmiss() == 0){
			countFlux.setMiss(detail.getBandwidth());
		}
		
		String domain = detail.getDomain();
		String cdnip = detail.getCdnip();
		int days = detail.getDay();
		int hours = detail.getHour();
		int minutes = detail.getMinute();
		int mark = detail.getMark();
		
		StringBuilder sb = new StringBuilder();
		sb.append(days).append("_").append(hours).append("_").append(minutes).append("_").append(domain).append("_").append(cdnip).append("_").append(mark);
		
		Tuple2<String, CountFlux> tuple2 = new Tuple2<String, CountFlux>(sb.toString(), countFlux);
		
		return tuple2;
	}
	
	public static CountFlux reduceByKey(CountFlux v1, CountFlux v2){
		v1.setBandwidth(v1.getBandwidth()+v2.getBandwidth());
		v1.setMiss(v1.getMiss()+v2.getMiss());
		return v1;
	}
	
	public static void insertSql(List<String> list, String sql){
		try {
			DBHelper dbHelper = DBHelper.getInstance();
			dbHelper.executeUpdate(sql);
		} catch (Exception e) {
			list.add(sql);
			log.error("execute sql fail:"+sql, e);
		}
	}
	
	public static Iterable<String> mapPartitions(Iterator<Tuple2<String, CountFlux>> it, String table){
		String insertSql = "INSERT INTO `"+table+"` (`agentid`, `bandwidth`, `cdnip`, `days`, `domain`, `hours`, `mark`, `minute`, `miss`, `months`, `serviceid`, `userid`, `years`) VALUES ";
		StringBuilder sb = new StringBuilder();
		int index = 0;
		List<String> list = new ArrayList<String>(0);
		while(it.hasNext()){
			CountFlux countFlux = it.next()._2;
			if(sb.length() == 0){
				sb.append(insertSql);
			}else{
				sb.append(",");
			}
			sb.append("('").append(countFlux.getAgentid()).append("'").append(",");
			sb.append("'").append(countFlux.getBandwidth()*8).append("'").append(",");
			sb.append("'").append(countFlux.getCdnip()).append("'").append(",");
			sb.append("'").append(countFlux.getDays()).append("'").append(",");
			sb.append("'").append(countFlux.getDomain()).append("'").append(",");
			sb.append("'").append(countFlux.getHours()).append("'").append(",");
			sb.append("'").append(countFlux.getMark()).append("'").append(",");
			sb.append("'").append(countFlux.getMinute()).append("'").append(",");
			sb.append("'").append(countFlux.getMiss()*8).append("'").append(",");
			sb.append("'").append(countFlux.getMonths()).append("'").append(",");
			sb.append("'").append(countFlux.getServiceid()).append("'").append(",");
			sb.append("'").append(countFlux.getUserid()).append("'").append(",");
			sb.append("'").append(countFlux.getYears()).append("')");
			
			if(++index >= 10000){
				String sql = sb.toString();
				insertSql(list,sql);
				sb.delete(0, sb.length());
				index = 0;
			}
		}
		if(sb.length() != 0){
			String sql = sb.toString();
			insertSql(list,sql);
		}
		return list;
	}
}
