package org.plugbase.analysis.entity;

/**
 * 原cdn_reptile_details表
 */
public class CountSpider {
	private int number;			//次数
	private String created ;	//创建时间年-月-日
	private int type ;			//爬虫类型（google，百度）
	private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int domain;			//域名
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int serviceid;		//服务ID
	
	public CountSpider(int number, String created, int type, int mark,
			int domain, int agentid, int userid, int serviceid) {
		super();
		this.number = number;
		this.created = created;
		this.type = type;
		this.mark = mark;
		this.domain = domain;
		this.agentid = agentid;
		this.userid = userid;
		this.serviceid = serviceid;
	}

	public CountSpider() {
		// TODO Auto-generated constructor stub
	}
	
	public int getNumber() {
		return number;
	}
	public void setNumber(int number) {
		this.number = number;
	}
	public String getCreated() {
		return created;
	}
	public void setCreated(String created) {
		this.created = created;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public int getMark() {
		return mark;
	}
	public void setMark(int mark) {
		this.mark = mark;
	}
	public int getDomain() {
		return domain;
	}
	public void setDomain(int domain) {
		this.domain = domain;
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
