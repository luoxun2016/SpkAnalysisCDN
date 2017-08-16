/**
 * @version $Id: Configuration.java v,1.0.0 2009-9-15 上午10:29:27 WangWentong $
 * @copyright (c) 2009 北京叁加伍网络科技有限公司
 * @link http://java.plugbase.org
 * 
 * AnalysisCDNLogs 功能描述
 */
package org.plugbase.analysis.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.plugbase.analysis.util.HadoopFileSystem;

/**
 * @author WangWentong
 * 
 */
public class Configuration {
	private static Logger logger = Logger.getLogger(Configuration.class);

	public static final String HFDS_NAME			= "hdfs://spark0:9000";
	public static final String FLUX_LOG				= "flux_log";
	public static final String ERR_DOMAIN			= "err_domain";
	public static final String COUNT_FLUX			= "count_flux";
	public static final String COUNT_INFO			= "count_info";
	public static final String COUNT_HIT			= "count_hit";
	public static final String COUNT_ISP			= "count_isp";
	public static final String COUNT_STATE			= "count_state";
	public static final String COUNT_AREA			= "count_area";
	public static final String COUNT_SPIDER			= "count_spider";
	public static final String COUNT_IP				= "count_ip";
	public static final String COUNT_REFFER			= "count_reffer";	//可选
	public static final String COUNT_HTTP			= "count_http";		//可选
	public static final String COUNT_EXT			= "count_ext";		//可选
	public static final String COUNT_URL			= "count_url";		//可选
	public static final String COUNT_DOWN			= "count_down";		//可选
	public static final String ERROR_SQL			= "error_sql";
	
	public static final String DO_LOG				= ".log";		//日志文件
	public static final String DO_COLLECT			= ".collect";	//正在汇总的文件
	public static final String DO_FINSH				= ".finsh";		//汇总完成的文件
	public static final String DO_WRITE				= ".write";		//正在汇总日志写日志的文件
	public static final String DO_REMOVE			= ".remove";	//将要删除的文件
	public static final String DO_PROGRESS			= ".progress";	//正在执行操作的文件
	public static final String DO_TEMP				= ".tmp";		//临时文件
	public static final String DO_SQL				= ".sql";		//SQL文件
	
	public static final String ROOT_PATH			= HFDS_NAME + "/cdn";
	public static final String INPUT_PATH			= ROOT_PATH + "/input";
	public static final String OUTPUT_PATH			= ROOT_PATH + "/output";
	public static final String TEMP_PATH			= ROOT_PATH + "/temp";
	public static final String ERROR_PATH			= ROOT_PATH + "/error";
	
	public static final String JDBC_USER			= "root";
	public static final String JDBC_PASS			= "root";
	public static final String JDBC_URL				= "jdbc:mysql://192.168.1.44:3306/cdn_test?characterEncoding=UTF-8";
	
	public static final String TOPIC_CDNLOG			= "topic-cdnlog3";
	
	private static final String CONF_PATH 			= "/cdn/conf/conf.properties";
	private static final String CONF_KEY_DOMAIN 	= "domainpath";
	private static final String CONF_KEY_NODE 		= "nodepath";
	
	private static String appName					 = "CDN Analysis";
	private static Map<String, Sourcehost> domainMap = null;//<domian, Sourcehost>
	private static Set<String> chinaNodeSet 		 = null;//国内节点
	private static Properties conf					 = null;//配置
	
	private static boolean debug					 = false;

	static {
		try {
			logger.info("Configuration Init!");
			
			conf = HadoopFileSystem.getProperties(CONF_PATH);
			
			domainMap 		= loadDomainMap(conf);
			chinaNodeSet 	= loadChinaNode(conf);
		} catch (Exception e) {
			logger.error(CONF_PATH +" load error!", e);
			throw new RuntimeException(e);
		}
	}

	public static Map<String, Sourcehost> loadDomainMap(Properties conf){
		Map<String, Sourcehost> map = new HashMap<String, Sourcehost>();
		
		String path	= conf.getProperty(CONF_KEY_DOMAIN);
		if(path == null || path.isEmpty()) {
			logger.info("######################"+CONF_KEY_DOMAIN+" is null or file not exist#############");
			return map;
		}
		
		String str = HadoopFileSystem.readFileToString(path);
		if(str.isEmpty()) return map;
		
		Pattern patt = Pattern.compile("\\|");
		String[] ss = str.split("\n");
		for(int i = 0 ; i < ss.length ; i++){
			String s = ss[i].trim();
			if(s.isEmpty()) continue;
			
			String[] ds = patt.split(s);
			if(ds.length < 5) continue;
			
			String domain 		= ds[0].trim();
			int serviceid 		= Integer.parseInt(ds[1].trim());
			int agentid 		= Integer.parseInt(ds[2].trim());
			int userid 			= Integer.parseInt(ds[3].trim());
			double flowrate 	= Double.parseDouble(ds[4].trim());
			
			Sourcehost host = new Sourcehost();
			host.setAgentid(agentid);
			host.setAllowdomain(domain);
			host.setServiceid(serviceid);
			host.setUserid(userid);
			host.setFlowrate(flowrate);
			host.setFlood(domain.startsWith("*"));
			
			map.put(domain, host);
		}
		
		return map;
	}
	
	public static Set<String> loadChinaNode(Properties conf){
		Set<String> nodeSet = new HashSet<String>();
		
		String path = conf.getProperty(CONF_KEY_NODE);
		
		if(path == null || path.isEmpty()) {
			logger.info("######################"+CONF_KEY_NODE+" is null or file not exist#############");
			return nodeSet;
		}
		
		String str =  HadoopFileSystem.readFileToString(path);
		if(str.isEmpty()) return nodeSet;
		
		String[] ss =  str.split("\n");
		for( int i = 0 ; i < ss.length ; i++ ){
			String s = ss[i].trim();
			if(s.isEmpty()) continue;
			
			nodeSet.add(s);
		}
		
		return nodeSet;
	}
	
	/**
	 * 获取域名详情
	 * @param domain
	 * @return
	 */
	public static Sourcehost getSourcehost(String domain){
		Sourcehost host = domainMap.get(domain);
		if(host == null){
			String[] domainArray=domain.split("\\.");
			int len = domainArray.length;
			if(len > 2){
				String tempDomain = "";
				String suffix = "." + domainArray[len-1];
				for(int i = len - 2 ; i > 0 ; i--){
					tempDomain = "." + domainArray[i] + tempDomain;
					String key = "*" + tempDomain + suffix;
					host = domainMap.get( key );
					if(host != null) return host;
				}
			}
			host = domainMap.get("*."+domain);
		}
		
		return host;
	}
	
	/**
	 * 判断是否是国内节点
	 * @param node
	 * @return
	 */
	public static boolean isChinaNode(String node){
		return chinaNodeSet.contains(node);
	}
	
	public static String getString(String key){
		return getString(key, null);
	}
	
	public static Integer getInt(String key){
		return getInt(key, 0);
	}
	
	public static String getString(String key, String defaultValue){
		String value = null;
		if(conf != null){
			value = conf.getProperty(key);
		}
		if(value == null){
			value = defaultValue;
		}
		return value;
	}
	
	public static Integer getInt(String key, Integer defaulValue){
		Integer value = null;
		if(conf != null){
			String strValue = conf.getProperty(key);
			if(strValue == null){
				value = defaulValue;
			}else{
				value = Integer.parseInt(strValue);
			}
		}
		if(value == null){
			value = defaulValue;
		}
		return value;
	}
	
	public static String getAppName() {
		return appName;
	}

	public static void setAppName(String appname) {
		Configuration.appName = appname;
	}
	
	public static boolean isDebug() {
		return debug;
	}

	public static void setDebug(boolean debug) {
		Configuration.debug = debug;
	}

	public static void main(String[] args) {
		getSourcehost("cyxm.959.cn");
	}
}
