package org.plugbase.analysis.entity;

/**
 * 原cdn_url表
 */
public class CountUrl {
	private String url;			//访问url
	private int urlh;			//访问url哈希
	private long visitcount;	//访问次数
	private int inbound;		//入站数（非站内访问次数）
	private String created;		//创建日期
	private String domainname;	//域名
	private int domain;			//域名哈希
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int serviceid;		//服务ID
	private int mark;			//国内外标识，国内HTTPS：14国内HTTP：13国外HTTPS：24,国外HTTP：23

	public CountUrl(String url, int urlh, long visitcount, int inbound, String created, String domainname, int domain, int agentid, int userid, int serviceid, int mark) {
		super();
		this.url = url;
		this.urlh = urlh;
		this.visitcount = visitcount;
		this.inbound = inbound;
		this.created = created;
		this.domainname = domainname;
		this.domain = domain;
		this.agentid = agentid;
		this.userid = userid;
		this.serviceid = serviceid;
		this.mark = mark;
	}

	public CountUrl(CountDetail detail) {
		this.visitcount=1;
		this.inbound=detail.getDomain().equals(detail.getReffer()) ? 0 : 1;
		this.created = detail.getCreated();
		this.domainname=detail.getDomain();
		this.domain = detail.getDomain().hashCode();
		this.url=detail.getUrl();
		this.urlh = detail.getUrl().hashCode();
		this.agentid = detail.getAgentid();
		this.userid = detail.getUserid();
		this.serviceid = detail.getServiceid();
		this.mark = detail.getMark();
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public long getVisitcount() {
		return visitcount;
	}

	public void setVisitcount(long visitcount) {
		this.visitcount = visitcount;
	}

	public int getInbound() {
		return inbound;
	}

	public void setInbound(int inbound) {
		this.inbound = inbound;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public String getDomainname() {
		return domainname;
	}

	public void setDomainname(String domainname) {
		this.domainname = domainname;
	}

	public int getUrlh() {
		return urlh;
	}

	public void setUrlh(int urlh) {
		this.urlh = urlh;
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

	public int getMark() {
		return mark;
	}

	public void setMark(int mark) {
		this.mark = mark;
	}
}
