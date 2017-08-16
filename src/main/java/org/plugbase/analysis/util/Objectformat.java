/**
 * @version $Id: Stringformat.java v,1.0.0 2009-9-15 下午03:52:54 WangWentong $
 * @copyright (c) 2009 北京叁加伍网络科技有限公司
 * @link http://java.plugbase.org
 * 
 * AnalysisCDNLogs 功能描述
 */
package org.plugbase.analysis.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author WangWentong
 *
 */
public class Objectformat {
	
	private static final SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static String formatNum(int num){
		String numstr = String.valueOf(num);
		if(numstr.length()<2){
			return "0"+numstr;
		}
		return numstr;
	}
	
	public static int defferDay(Date sdate , Date odate){
		Calendar calendar1 = Calendar.getInstance();
		Calendar calendar2 = Calendar.getInstance();
		calendar1.setTime(sdate);
		calendar2.setTime(odate);
		return calendar1.get(Calendar.DAY_OF_MONTH)-calendar2.get(Calendar.DAY_OF_MONTH);		
	}
	
	public static String formatDate(Date date){
		return dateformat.format(date);
	}
}
