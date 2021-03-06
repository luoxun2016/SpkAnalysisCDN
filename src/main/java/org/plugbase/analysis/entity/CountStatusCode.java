package org.plugbase.analysis.entity;

/**
 *  cdn_statuscode表
 *  codeid bigint(20) NOT NULL
	domain int(11) NULL域名
	name smallint(6) NULL状态码
	number int(11) NULL请求次数
	flux double NULL流量
	ipcount bigint(11) NULL独立IP个数
	created int(15) NULL日期
	serviceid int(11) NULL
	agentid int(11) NULL
	userid int(11) NULL
	mark int(11) NULL国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
 *
 */
public class CountStatusCode {
	private int domain;		//域名哈希
	private int name;		//状态码
	private long number;	//请求次数
	private double flux;	//下行流量
	private long ipcount;	//有多少个不相同的IP
	private String created;	//时间
	private int serviceid;	//服务ID
	private int agentid;	//代理商ID
	private int userid;		//用户ID
	private int mark;		//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
	
	public CountStatusCode(int domain, int name, long number, double flux,
			long ipcount, String created, int serviceid, int agentid,
			int userid, int mark) {
		super();
		this.domain = domain;
		this.name = name;
		this.number = number;
		this.flux = flux;
		this.ipcount = ipcount;
		this.created = created;
		this.serviceid = serviceid;
		this.agentid = agentid;
		this.userid = userid;
		this.mark = mark;
	}
	
	public int getDomain() {
		return domain;
	}
	public void setDomain(int domain) {
		this.domain = domain;
	}
	public int getName() {
		return name;
	}
	public void setName(int name) {
		this.name = name;
	}
	public long getNumber() {
		return number;
	}
	public void setNumber(long number) {
		this.number = number;
	}
	public double getFlux() {
		return flux;
	}
	public void setFlux(double flux) {
		this.flux = flux;
	}
	public long getIpcount() {
		return ipcount;
	}
	public void setIpcount(long ipcount) {
		this.ipcount = ipcount;
	}
	public String getCreated() {
		return created;
	}
	public void setCreated(String created) {
		this.created = created;
	}
	public int getServiceid() {
		return serviceid;
	}
	public void setServiceid(int serviceid) {
		this.serviceid = serviceid;
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
	public int getMark() {
		return mark;
	}
	public void setMark(int mark) {
		this.mark = mark;
	}
	
}
