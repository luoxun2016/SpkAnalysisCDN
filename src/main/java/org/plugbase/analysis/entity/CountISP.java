package org.plugbase.analysis.entity;

/**
 *  cdn_isp表
 *  ispid bigint(20) NOT NULL
	domain int(100) NULL域名hashcode
	name int(20) NULL运营商
	number int(11) NULL点击量
	ipcount int(11) NULL独立IP
	flux double NULL流量
	created int(15) NULL
	serviceid int(11) NULL
	agentid int(11) NULL
	userid int(11) NULL
	mark int(11) NULL国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
 *
 */
public class CountISP {
	private int domain;		//域名哈希
	private short name;		//运营商
	private long number;	//点击量
	private long ipcount;	//不相同IP数据
	private double flux;	//下行流量
	private String created;	//时间
	private int serviceid;	//服务ID
	private int agentid;	//代理商ID
	private int userid;		//用户ID
	private int mark;		//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
	
	public CountISP(int domain, short name, long number, long ipcount,
			double flux, String created, int serviceid, int agentid, int userid,
			int mark) {
		super();
		this.domain = domain;
		this.name = name;
		this.number = number;
		this.ipcount = ipcount;
		this.flux = flux;
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
	public short getName() {
		return name;
	}
	public void setName(short name) {
		this.name = name;
	}
	public long getNumber() {
		return number;
	}
	public void setNumber(long number) {
		this.number = number;
	}
	public long getIpcount() {
		return ipcount;
	}
	public void setIpcount(long ipcount) {
		this.ipcount = ipcount;
	}
	public double getFlux() {
		return flux;
	}
	public void setFlux(double flux) {
		this.flux = flux;
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
