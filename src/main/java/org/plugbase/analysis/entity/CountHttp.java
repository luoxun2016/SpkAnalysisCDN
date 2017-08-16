package org.plugbase.analysis.entity;

/**
 * 原cdn_日期_http
 */
public class CountHttp {
	private double flux;		//下行流量
	private long httprequest;	//次数统计
	private int filecount;		//文件请求次数
	private int pagecount;		//页面请求次数
	private int year;			//年
	private int month;			//月
	private int day;			//日
	private int hour;			//时
	private String domainname;	//域名
	private int domain;			//域名hashcode
	private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int serviceid;		//服务ID
	
	public CountHttp(double flux, long number, int filenum, int webnum,
			int year, int month, int day, int hour, String domainname, int domain, int mark,
			int agentid, int userid, int serviceid) {
		super();
		this.flux = flux;
		this.httprequest = number;
		this.filecount = filenum;
		this.pagecount = webnum;
		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
		this.domainname = domainname;
		this.domain = domain;
		this.mark = mark;
		this.agentid = agentid;
		this.userid = userid;
		this.serviceid = serviceid;
	}

	public CountHttp(CountDetail detail) {
		this.httprequest = 1;
		if (detail.getFiletype() == 0) {
			this.filecount = 1;
		} else {
			this.pagecount = 1;
		}
		this.flux = detail.getBandwidth();
		this.year = detail.getYear();
		this.month = detail.getMonth();
		this.day = detail.getDay();
		this.hour = detail.getHour();
		this.domainname = detail.getDomain();
		this.domain = detail.hashCode();
		this.mark = detail.getMark();
		this.serviceid = detail.getServiceid();
		this.agentid = detail.getAgentid();
		this.userid = detail.getUserid();
	}

	public double getFlux() {
		return flux;
	}

	public void setFlux(double flux) {
		this.flux = flux;
	}

	public long getHttprequest() {
		return httprequest;
	}

	public void setHttprequest(long httprequest) {
		this.httprequest = httprequest;
	}

	public int getFilecount() {
		return filecount;
	}

	public void setFilecount(int filecount) {
		this.filecount = filecount;
	}

	public int getPagecount() {
		return pagecount;
	}

	public void setPagecount(int pagecount) {
		this.pagecount = pagecount;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		this.hour = hour;
	}

	public String getDomainname() {
		return domainname;
	}

	public void setDomainname(String domainname) {
		this.domainname = domainname;
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
