package org.plugbase.analysis.entity;

/**
 * 原cdn_hit表
 */
public class CountHit {
	private int domain;			//域名
	private String created;		//创建日期
	private long hitcount;		//命中次数
	private long hitallcount;	//总点击数
	private double flux;		//命中流量
	private double fluxall;		//总流量
	private int serviceid;		//服务ID
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int mark;			//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23
	
	public CountHit(int domain, String created, long hitcount, long hitallcount,
			double flux, double fluxall, int serviceid, int agentid,
			int userid, int mark) {
		super();
		this.domain = domain;
		this.created = created;
		this.hitcount = hitcount;
		this.hitallcount = hitallcount;
		this.flux = flux;
		this.fluxall = fluxall;
		this.serviceid = serviceid;
		this.agentid = agentid;
		this.userid = userid;
		this.mark = mark;
	}
	
	public CountHit(CountDetail detail) {
		this.domain = detail.getDomain().hashCode();
		this.created = detail.getCreated();
		this.hitcount = detail.getHitmiss() == 1 ? 1 : 0;
		this.hitallcount = 1;
		this.flux = detail.getHitmiss() == 1 ? detail.getBandwidth() : 0;
		this.fluxall = detail.getBandwidth();
		this.serviceid = detail.getServiceid();
		this.agentid = detail.getAgentid();
		this.userid = detail.getUserid();
		this.mark = detail.getMark();
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

	public long getHitcount() {
		return hitcount;
	}

	public void setHitcount(long hitcount) {
		this.hitcount = hitcount;
	}

	public long getHitallcount() {
		return hitallcount;
	}

	public void setHitallcount(long hitallcount) {
		this.hitallcount = hitallcount;
	}

	public double getFlux() {
		return flux;
	}
	public void setFlux(double flux) {
		this.flux = flux;
	}
	public double getFluxall() {
		return fluxall;
	}
	public void setFluxall(double fluxall) {
		this.fluxall = fluxall;
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
