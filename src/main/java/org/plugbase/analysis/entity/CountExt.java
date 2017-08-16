package org.plugbase.analysis.entity;

/**
 * 原cdn_countext表
 */
public class CountExt {
	private double flux;		// 下行流量
	private long ip;			// 访问IP
	private int ext;			// 扩展类型1：图片 0：其它
	private int ipcount;		// 次数统计
	private int years; 			// 年
	private int months; 		// 月
	private int days; 			// 日
	private String domainname; 	// 域名
	private int domain;			// 域名hashcode
	private int mark; 			// 13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int agentid; 		// 代理商ID
	private int userid; 		// 用户ID
	private int serviceid;  	// 服务ID
	
	public CountExt(double flux, long ip, int ext, int ipcount, int year,
			int month, int day, String domainname, int domain, int mark, int agentid,
			int userid, int serviceid) {
		super();
		this.flux = flux;
		this.ip = ip;
		this.ext = ext;
		this.ipcount = ipcount;
		this.years = year;
		this.months = month;
		this.days = day;
		this.domainname = domainname;
		this.domain = domain;
		this.mark = mark;
		this.agentid = agentid;
		this.userid = userid;
		this.serviceid = serviceid;
	}

	public CountExt(CountDetail detail) {
		this.domainname = detail.getDomain();
		this.domain = detail.getDomain().hashCode();
		this.years = detail.getYear();
		this.months = detail.getMonth();
		this.days = detail.getDay();
		this.flux = detail.getBandwidth();
		this.ext = detail.getExt();
		this.mark = detail.getMark();
		this.serviceid = detail.getServiceid();
		this.agentid = detail.getAgentid();
		this.userid = detail.getUserid();
		this.ip = detail.getLongip();
		this.ipcount = 1;
	}

	public double getFlux() {
		return flux;
	}

	public void setFlux(double flux) {
		this.flux = flux;
	}

	public long getIp() {
		return ip;
	}

	public void setIp(long ip) {
		this.ip = ip;
	}

	public int getExt() {
		return ext;
	}

	public void setExt(int ext) {
		this.ext = ext;
	}

	public int getIpcount() {
		return ipcount;
	}

	public void setIpcount(int ipcount) {
		this.ipcount = ipcount;
	}

	public int getYears() {
		return years;
	}

	public void setYears(int year) {
		this.years = year;
	}

	public int getMonths() {
		return months;
	}

	public void setMonths(int month) {
		this.months = month;
	}

	public int getDays() {
		return days;
	}

	public void setDays(int day) {
		this.days = day;
	}

	public String getDomainname() {
		return domainname;
	}

	public void setDomainname(String domain) {
		this.domainname = domain;
	}
	
	public int getDomain() {
		return domain;
	}

	public void setDomain(int domain) {
		this.domain = domain;
	}

	public int getMark() {
		return mark;
	}

	public void setMark(int mark) {
		this.mark = mark;
	}

	public int getAgentid() {
		return agentid;
	}

	public void setAgentid(int agentid) {
		this.agentid = agentid;
	}

	public int getUserid() {
		return userid;
	}

	public void setUserid(int userid) {
		this.userid = userid;
	}

	public int getServiceid() {
		return serviceid;
	}

	public void setServiceid(int serviceid) {
		this.serviceid = serviceid;
	}
	
}
