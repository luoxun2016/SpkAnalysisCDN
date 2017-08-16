package org.plugbase.analysis.entity;

/**
 * 原cdn_domainip
 */
public class CountIP {
	private int ipcount;	// 次数统计
	private double flux; 	// 下行流量
	private long ip; 		// 访问IP to Long
	private String created;	// 创建日期
	private int domain; 	// 域名
	private int mark; 		// 13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int agentid; 	// 代理商ID
	private int userid; 	// 用户ID
	private int serviceid; 	// 服务ID
	
	public CountIP(int ipcount, double flux, long ip, String created, int domain, int mark, int agentid,
			int userid, int serviceid) {
		super();
		this.ipcount = ipcount;
		this.flux = flux;
		this.ip = ip;
		this.created = created;
		this.domain = domain;
		this.mark = mark;
		this.agentid = agentid;
		this.userid = userid;
		this.serviceid = serviceid;
	}

	public CountIP(CountDetail detail) {
		this.ipcount = 1;
		this.ip = detail.getLongip();
		this.created = detail.getCreated();
		this.domain = detail.getDomain().hashCode();
		this.flux = detail.getBandwidth();
		this.mark = detail.getMark();
		this.serviceid = detail.getServiceid();
		this.agentid = detail.getAgentid();
		this.userid = detail.getUserid();
	}
	
	public int getIpcount() {
		return ipcount;
	}
	
	public void setIpcount(int ipcount) {
		this.ipcount = ipcount;
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

	public int getDomain() {
		return domain;
	}

	public void setDomain(int domain) {
		this.domain = domain;
	}
	
	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
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
