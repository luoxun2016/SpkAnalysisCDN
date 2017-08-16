package org.plugbase.analysis.entity;

/**
 *  表cdn_area
 * 	areaid bigint(20) NOT NULL
	domain int(100) NULL域名hashcode
	name int(20) NULL区域名称
	number int(11) NULL点击量
	created int(15) NULL日期
	ipcount int(11) NULL独立IP
	bandwidth double NULL流量
	serviceid int(11) NULL
	agentid int(11) NULL
	userid int(11) NULL
	mark int(11) NULL国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
 *
 */
public class CountArea {
	private int domain;			//域名哈希
	private int name;			//区域码
	private long number;			//点击量
	private String created;		//创建时间
	private long ipcount;		//多少个不相同IP
	private double bandwidth;	//下行流量
	private int serviceid;		//服务ID
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int mark;			//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
	
	public CountArea(int domain, int name, long number, String created,
			long ipcount, double bandwidth, int serviceid, int agentid,
			int userid, int mark) {
		super();
		this.domain = domain;
		this.name = name;
		this.number = number;
		this.created = created;
		this.ipcount = ipcount;
		this.bandwidth = bandwidth;
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

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public long getIpcount() {
		return ipcount;
	}

	public void setIpcount(long ipcount) {
		this.ipcount = ipcount;
	}

	public double getBandwidth() {
		return bandwidth;
	}

	public void setBandwidth(double bandwidth) {
		this.bandwidth = bandwidth;
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
