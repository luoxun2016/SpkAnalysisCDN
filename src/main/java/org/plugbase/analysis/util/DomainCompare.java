package org.plugbase.analysis.util;

import java.util.regex.Pattern;

/**
 * 
 * @version DomainCompare.java v. 1.0.0 2010-7-26
 * @author CaoShiChao
 * copyright (c) 2010 北京叁加伍网络科技有限公司   http://www.3jia5.com
 */
public class DomainCompare {
	private static final Pattern patt = Pattern.compile("\\.");
	
	public static String levelDomain(String  domain){	
		String[] domains=patt.split(domain);
		String temp=domain;
		if(domain.contains(".com.cn")||domain.contains(".net.cn")||domain.contains(".org.cn")||domain.contains(".gov.cn") || domain.contains(".edu.cn")){
			if(domains.length>3){
			temp=domains[domains.length-3]+"."+domains[domains.length-2]+"."+domains[domains.length-1];
		}
		}else{
			if(domains.length>2){
			temp=domains[domains.length-2]+"."+domains[domains.length-1];
			}
		}
		
		return temp;		
	}
	public static void main(String[] args) {
		System.out.println(levelDomain("www.3158.cn"));
	}
}
