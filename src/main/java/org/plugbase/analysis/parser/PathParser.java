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

public class PathParser {
	private static Logger logger = Logger.getLogger(PathParser.class);
	
	private static final String HTCP_CLR 	= "HTCP_CLR";
	
	public static String parse(String logline) {
		int len = logline.length();
		int i=0;
		char c;
		int year , month , day, hour, minute;
		year = month = day = hour = minute = 0;
		String state, domain, sflux, created, method;
		domain = sflux = state = created = method = "";

		StringBuilder sb = new StringBuilder(logline.length());
		
		//读取访问IP
		while( i < len && (c=logline.charAt(i)) != ' ')  {i++;}
		
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
		while( i < len && (c=logline.charAt(i)) != ' ')  {i++;}
		while( i < len && (c=logline.charAt(i)) != ' ')  {
			sb.append(c);
			i++;
		}
		method  =toStringAndClear(sb);
		if(method.equals(HTCP_CLR)) return null;
		
		//读取协议头http:、https:
		while( i < len && (c=logline.charAt(i)) != '/')  {i++;}
		
		//读取域名
		while( i < len && (c=logline.charAt(i)) == '/')  {i++;}
		while( i < len && (c=logline.charAt(i)) != '/'){
			sb.append(c);
			i++;
		}
		domain = toStringAndClear(sb);
		
		//读取url的query部分
		while (i < len && (c = logline.charAt(i)) == '/') {i++;}
		while (i < len && (c = logline.charAt(i)) != ' ') {i++;}
		
		while( i < len && (c = logline.charAt(i)) == ' ')  {i++;}
		while( i < len && (c = logline.charAt(i)) != '"')  {i++;}
		while( i < len && (c = logline.charAt(i)) == '"')  {i++;}
		while( i < len && (c = logline.charAt(i)) == ' ')  {i++;}
		
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
		
		//获取域名相关信息
		Sourcehost host = Configuration.getSourcehost(domain);
		if(host==null){
			return null;
		}
		
		StringBuilder filename = new StringBuilder();
		filename.append(created).append("/").append(host.getAllowdomain()).append(".log");
		
		return filename.toString();
	}
	
	private static String toStringAndClear(StringBuilder sb){
		String s = sb.toString();
		sb.delete(0, sb.length());
		return s;
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
		logline = "106.120.188.154 - - 2016-08-15 00:31:10 \"GET http://19978.cn/robots.txt HTTP/1.1\" 301 522 \"-\" \"Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)\" TCP_MISS:FIRSTUP_PARENT 190 397 33 36.110.137.103";
//		logline = "101.206.139.102 - - 2016-08-15 00:16:28 \"GET http://19978.cn/ HTTP/1.1\" 301 553 \"http://icp.chinaz.com/info?q=19978.cn\" \"Mozilla/5.0 (Linux; Android 4.4.4; Hisense E621T Build/KTU84P) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36 baiduboxapp/5.0 (Baidu; P1 4.4.4)\" TCP_MISS:FIRSTUP_PARENT 762 326 22 222.140.154.196";
		long start = System.currentTimeMillis();
		for(int i = 0 ; i < 1000000 ; i++){
			PathParser.parse(logline);
		}
		System.out.println(System.currentTimeMillis()-start);
	}
}
