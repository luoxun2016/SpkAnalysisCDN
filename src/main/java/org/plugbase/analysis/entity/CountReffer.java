package org.plugbase.analysis.entity;

/**
 * 原cdn_comefroms表
 */
public class CountReffer {
	private String created;	//时间年-月-日
	private long number;	//次数
	private String reffer;	//来源
	private int domain;		//域名
	private int mark;		//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int agentid;	//代理商ID
	private int userid;		//用户ID
	private int serviceid;	//服务ID
	
	public CountReffer(String created, long number, String reffer, int domain,
			int mark, int agentid, int userid, int serviceid) {
		super();
		this.created = created;
		this.number = number;
		this.reffer = reffer;
		this.domain = domain;
		this.mark = mark;
		this.agentid = agentid;
		this.userid = userid;
		this.serviceid = serviceid;
	}

	public CountReffer(CountDetail detail, String reffer, String date) {
		this.number = 1;
		this.reffer = reffer;
		this.created = date;
		this.domain = detail.getDomain().hashCode();
		this.mark = detail.getMark();
		this.serviceid = detail.getServiceid();
		this.userid = detail.getUserid();
		this.agentid = detail.getAgentid();
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public long getNumber() {
		return number;
	}

	public void setNumber(long number) {
		this.number = number;
	}

	public String getReffer() {
		return reffer;
	}

	public void setReffer(String reffer) {
		this.reffer = reffer;
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

	public int getUserid() {
		return userid;
	}

	public void setUserid(int userid) {
		this.userid = userid;
	}

	public int getAgentid() {
		return agentid;
	}

	public void setAgentid(int agentid) {
		this.agentid = agentid;
	}

	public int getServiceid() {
		return serviceid;
	}

	public void setServiceid(int serviceid) {
		this.serviceid = serviceid;
	}
	
}
