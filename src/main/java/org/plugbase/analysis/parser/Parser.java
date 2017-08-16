package org.plugbase.analysis.parser;

import static org.plugbase.analysis.config.Configuration.HFDS_NAME;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.config.Sourcehost;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.util.SpiderUtil;
import org.plugbase.util.ip.IPTable;
import org.plugbase.util.ip.IPTable.IPInfo;
import org.plugbase.util.ip.IPUtils;

public class Parser {
	
	private static Logger logger = Logger.getLogger(Parser.class);
	
	private static final char spiderChar 	= '-' ;
	private static final String HTCP_CLR 	= "HTCP_CLR";
	private static final String PowerPurge 	= "PowerPurge";
	
	public static CountDetail parse(String logline) {
		int len = logline.length();
		int i=0;
		int ii=0;
		char c,ct;
		int year , month , day, hour, minute, t1,  pos, point, filetype, downtime, looptime, statecode;
		year = month = day = hour = minute = t1 =  pos = point = filetype = downtime = looptime = statecode = 0;
		String state, domain, sflux, ip, reffer, hitmiss, url, ext, ssl, supflux, cdnip, spider, created, method, agent;
		domain = sflux = state = ip = reffer = hitmiss = url = ext = ssl = supflux = cdnip = spider = created = method= agent = "";

		StringBuilder sb = new StringBuilder(logline.length()/2);
		
		//读取访问IP
		while(i < len && (c=logline.charAt(i)) != ' ')  {
			sb.append(c);
			i++;
		}
		ip = toStringAndClear(sb);
		
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) != ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) != ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
	
		//读取年、月、日
		int k = 0;
		while(i < len && (c = logline.charAt(i)) != ' ') {
			if(c == '-') {
				k++;
			}else {
				if(k == 0){
					sb.append(c);
					year = year * 10 + (c - '0');
				} 
				else if(k == 1){
					sb.append(c);
					month = month * 10 + (c - '0');
				}
				else if(k == 2){
					sb.append(c);
					day = day * 10 + (c - '0');
				} 
			}
			i++;
		}
		created = toStringAndClear(sb);

		while(i < len && (c = logline.charAt(i)) == ' ') {i++;}
		
		//读取时、分
		k = 0;
		while(i < len && (c = logline.charAt(i)) != ' ') {
			if(c == ':') {
				k++;
			}else {
				if(k == 0) hour = hour * 10 + (c - '0');
				else if(k == 1) minute = minute * 10 + (c - '0');
			}
			i++;
		}
	
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) == '"')  {i++;}
		while( i < len && (c=logline.charAt(i)) != ' ')  {
			sb.append(c);
			i++;
		}
		method  =toStringAndClear(sb);
		if(method.equals(HTCP_CLR)) return null;
		
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		
		//读取协议头http:、https:
		while( i < len && (c=logline.charAt(i)) != '/')  {
			sb.append(c);
			i++;
		}
		ssl = toStringAndClear(sb);
		
		//读取域名
		while( i < len && (c=logline.charAt(i)) == '/')  {i++;}
		while( i < len && (c=logline.charAt(i)) != '/'){
			sb.append(c);
			i++;
		}
		domain = toStringAndClear(sb);
		
		//读取url的query部分
		while (i < len && (c = logline.charAt(i)) == '/') {i++;}
		while (i < len && (c = logline.charAt(i)) != ' ') {
			sb.append(c);
			i++;
		}
		url = toStringAndClear(sb);
		
		int lens = url.length();
		
		while(ii < lens)  { 
			ct =url.charAt(ii);
			if(ct == '/'){
				pos = ii;
			}
			if(ct == '.'){
				point = ii;
			}
			if(ct == '?' || ct == '#'){
				t1 = ii;
				break;
			}			
			ii++ ;
		}
		
		if(point>0){
			if(t1>0){
				ext =ext(url,point,t1);
			}else{
				ext =ext(url,point,lens);
			}
		}else{
			if(t1>0 || len==pos+1){
				filetype = 1;
			}
		}
		
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) != '"')  {i++;}
		while( i < len && (c=logline.charAt(i)) == '"')  {i++;}
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		
		//读取HTTP状态
		while( i < len && (c=logline.charAt(i)) != ' ' && (c >= '0' && c <= '9')){
			sb.append(c);
			i++;
		}
		state = toStringAndClear(sb);
		
		if( state.isEmpty() ){
			logger.debug( "log statecode error:" + logline ) ;
			return null;
		}
		
		statecode = Integer.parseInt(state);
		
		//读取下行流量
		while( i < len && (c = logline.charAt(i)) == ' ') {i++;}
		while( i < len && (c = logline.charAt(i)) != ' ' &&  ((c >= '0' && c <= '9') || c == '.' ) ) {
			sb.append(c);
			i++;
		}
		sflux = toStringAndClear(sb);
		
		if(sflux.equals("") || i >= len) {
			logger.debug("log flux error:" + logline);
			return null;
		}
		double flux = Double.parseDouble(sflux);
		
		//获取域名相关信息
		String errDomain = null;
		Sourcehost host = Configuration.getSourcehost(domain);
		if(host==null){
			if(statecode == 200){
				//TODO 此域名未加速或已取消加速但是有CDN日志
				errDomain = domain;
				host = new Sourcehost();
				host.setAllowdomain(domain);
			}else{
				return null;
			}
		}
		double flowrate = host.getFlowrate();
		if(flowrate!=1){
			flux = flux * flowrate;
		}
		
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		if   ( i < len && (c=logline.charAt(i)) == '"')  {i++;}
		char temp = 0 ;
		while( i < len && (c=logline.charAt(i)) != '/' && c != '\"')  {
			temp =logline.charAt(i);			
			i++;
			if( temp =='-') break;
		}
		
		//读取reffer
		if(!(temp=='-')){
			while( i < len && (c=logline.charAt(i)) == '/')  {i++;}
			while( i < len && (c = logline.charAt(i)) != '/'&& (c = logline.charAt(i)) != '\"'&& (c = logline.charAt(i)) != ' '){
				sb.append(c);
				i++;
			}
			reffer = toStringAndClear(sb);
			
			while( i < len && (c=logline.charAt(i)) == '/')  {i++;}
			while( i < len && (c=logline.charAt(i)) != '"')  {i++;}
		}
		
		//读取爬虫
		while( i < len && (c=logline.charAt(i)) == '"')  {i++;}
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		if   ( i < len && (c=logline.charAt(i)) == '"')  {i++;}
		
		if(temp == spiderChar){
			while( i < len && (c=logline.charAt(i)) != '"')  {
				sb.append(c);
				i++;
			}
			spider = toStringAndClear(sb);
		}else{
			while( i < len && (c=logline.charAt(i)) != '"')  {
				if(sb.length() <= PowerPurge.length()) sb.append(c);
				i++;
			}
			agent = toStringAndClear(sb);
			if(agent.equals(PowerPurge)) return null;
		}
		
		while( i < len && (c=logline.charAt(i)) == '"')  {i++;}
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		
		//读取HIT MISS
		while( i < len && (c=logline.charAt(i)) != ':')  {
			sb.append(c);
			i++;
		}	
		hitmiss = toStringAndClear(sb);
		
		//上行流量
		while( i < len && (c=logline.charAt(i)) != ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) != ' ' &&  ((c >= '0' && c <= '9') || c == '.') ) {
			sb.append(c);
			i++;
		}
		supflux = toStringAndClear(sb);
		double upflux = supflux.isEmpty() ? 0 : Double.parseDouble(supflux);
		
		//下行时间
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) != ' ')  {i++;}
		
		//回源时间
		while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) != ' ')  {i++;}
		
		//节点ip
		do{
			sb.delete(0, sb.length());
			
			while( i < len && (c=logline.charAt(i)) == ' ')  {i++;}
			while( i < len && (c=logline.charAt(i)) != ' ')  {
				sb.append(c);
				i++;
			}
		}while(i < len);
		cdnip = toStringAndClear(sb);
		
		
		//国内http(13)，国外http(14)，国内https(23)，国外https(24)加速
		int mark = getMark(cdnip,ssl);
		
		//文件类型 网页1 其它0
		if(!ext.trim().isEmpty()){
			filetype = filetype(ext);
		}
		
		//HIT MISS
		byte bhitmiss = 0;
		if(hitmiss.contains("HIT") || hitmiss.contains("UNMODIFIED")) {
			bhitmiss = 1;
		}
		
		//IP转Long
		long longip = IPUtils.addr2ip(ip);
		
		//查询访问IP运营商
		IPInfo info = IPTable.query(longip);
		byte isp 	= (byte) info.getIsp().getCode();
		int area 	= info.getLocal().getCode();
		
		//判断是否为有效的爬虫
		if(spider.length() <= 12 || spider.indexOf("PowerCDN") !=-1 ){
			spider = null;
		}
		
		//判断爬虫类型
		int spidertype = SpiderUtil.getSpiderType(spider);
		
		CountDetail countdetail = new CountDetail();
		countdetail.setIsp(isp);
		countdetail.setArea(area);
		countdetail.setLongip(longip);
		countdetail.setCdnip(cdnip);
		countdetail.setHitmiss(bhitmiss);
		countdetail.setMark(mark);
		countdetail.setExt(ispic(ext));
		countdetail.setFiletype(filetype);
		countdetail.setBandwidth(flux);
		countdetail.setReffer(reffer);
		countdetail.setIp(ip);
		countdetail.setDomain(host.getAllowdomain());
		countdetail.setMinute(minute / 5 * 5);
		countdetail.setHour(hour);
		countdetail.setDay(day);
		countdetail.setMonth(month);
		countdetail.setYear(year);
		countdetail.setState(statecode);
		countdetail.setServiceid(host.getServiceid());
		countdetail.setUserid(host.getUserid());
		countdetail.setAgentid(host.getAgentid());
		countdetail.setUrl(url);
		countdetail.setUpflux(upflux);
		countdetail.setDowntime(downtime);
		countdetail.setLooptime(looptime);
		countdetail.setSpider(spider);
		countdetail.setSpidertype(spidertype);
		countdetail.setCreated(created);
		
		//记录错误domain
		countdetail.setErrDomain(errDomain);
		
		return countdetail;
	}
	
	private static String toStringAndClear(StringBuilder sb){
		String s = sb.toString();
		sb.delete(0, sb.length());
		return s;
	}
	
	private static int getMark(String cdnip, String ssl) {
		int mark=20;//国外
		if(Configuration.isChinaNode(cdnip)) mark=10;//国内
		return mark=("https:".equalsIgnoreCase(ssl))?(mark+4):(mark+3);//3:无ssl，4：有ssl
	}
	
	private static int  ispic(String ext){		
		int filetype =0;
		if("jpg".equals(ext)){
			return 1;
		}else if("png".equals(ext)){
			return 1;
		}else if("gif".equals(ext)){
			return 1;
		}else if("bmp".equals(ext)){
			return 1;
		}else if("tiff".equals(ext)){
			return 1;
		}else if("psd".equals(ext)){
			return 1;
		}else if( "ai".equals(ext)){
			return 1;
		}else if("exif".equals(ext)){
			return 1;
		}else if("pcx".equals(ext)){
			return 1;
		}
		return filetype;
	}
	
	private static int  filetype(String ext){	
		int filetype =0;
		if("htm".equals(ext)){
			return 1;
		}else if("html".equals(ext)){
			return 1;
		}else if("xhtml".equals(ext)){
			return 1;
		}else if("asp".equals(ext)){
			return 1;
		}else if("aspx".equals(ext)){
			return 1;
		}else if( "php".equals(ext)){
			return 1;
		}else if("jsp".equals(ext)){
			return 1;
		}else if("shtml".equals(ext)){
			return 1;
		}
		return filetype;
	}
	
	private static String ext(String data, int start, int end) {
		try{
			return data.substring(start+1,end);
		}catch(Exception e){
			return "";
		}
	}
	
	public static void main(String[] args) throws IOException {
		Configuration.isChinaNode("test");
		
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		conf.set("fs.defaultFS", HFDS_NAME);
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path("/cdn/input/19978.cn_2016-08-15_7"));
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		int n = 0;
		String logline = null;
		while(true){
			logline = reader.readLine();
			if(n++ > 1)break;
		}
//		logline = "211.147.6.246 - - 2016-09-08 19:00:14 \"GET http://www.jjwxc.net/purge/onebook.php?novelid=2921899 HTTP/1.1\" 404 349 \"http://www.powercdn.com/\" \"PowerPurge\" -:TCP 194 0.000 - 211.147.6.246";
//		logline = "-	127.0.0.1 - - 2016-08-24 23:56:08 \"HTCP_CLR http://www.3158.cn/ HTTP/0.0\" 0 0 \"-\" \"-\" UDP_HIT:HIER_NONE 0 941 0 -";
//		logline = "101.206.139.102 - - 2016-08-15 00:16:28 \"GET http://19978.cn/ HTTP/1.1\" 301 553 \"http://icp.chinaz.com/info?q=19978.cn\" \"Mozilla/5.0 (Linux; Android 4.4.4; Hisense E621T Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 baiduboxapp/5.0 (Baidu; P1 4.4.4)\" TCP_MISS:FIRSTUP_PARENT 762 326 22 222.140.154.196";
//		logline = "101.19.198.172 - - 2016-08-01 06:23:23 \"GET http://m.52kkm.org/j/hengfu1.js HTTP/1.1\" 200 1101 \"\" \"\" HIT:TCP 470 0.000 - 111.206.73.237";
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 10000000; i++){
			Parser.parse(logline);
		}
		System.out.println(System.currentTimeMillis()-start);
	}
}
