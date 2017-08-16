package org.plugbase.analysis.entity;

/**
 * 原cdn_download表
 */
public class CountDown {
	private String domainname;	//域名
	private int domain;			//域名哈希
	private String fileurl;		//访问URL
	private int fileurlhash;	//访问URL哈希
	private double size;		//文件大小
	private double totalcount;	//总下载大小
	private int finishedcount;	//成功下载次数 且 是非站内下载
	private int convercount;	//总下载次数
	private String created;		//时间年月日
	private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int serviceid;		//服务ID
	
	public CountDown(String created, String fileurl, int fileurlhash, double size, int finishedcount,
			int convercount, double totalcount, String domainname, int domain, int mark,
			int agentid, int userid, int serviceid) {
		super();
		this.created = created;
		this.fileurl = fileurl;
		this.fileurlhash = fileurlhash;
		this.size = size;
		this.finishedcount = finishedcount;
		this.convercount = convercount;
		this.totalcount = totalcount;
		this.domainname = domainname;
		this.domain = domain;
		this.mark = mark;
		this.agentid = agentid;
		this.userid = userid;
		this.serviceid = serviceid;
	}

	public CountDown(CountDetail detail) {
		this.convercount = 1;
		this.created = detail.getCreated();
		this.fileurl = detail.getUrl();
		this.fileurlhash = detail.getUrl().hashCode();
		this.size = detail.getBandwidth();
		this.totalcount = detail.getBandwidth();
		this.finishedcount = ((detail.getState() == 200) && (!detail.getDomain().equals(detail.getReffer()))) ? 1 : 0;
		this.domainname = detail.getDomain();
		this.domain = detail.getDomain().hashCode();
		this.mark = detail.getMark();
		this.serviceid = detail.getServiceid();
		this.agentid = detail.getAgentid();
		this.userid = detail.getUserid();
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public String getFileurl() {
		return fileurl;
	}

	public void setFileurl(String fileurl) {
		this.fileurl = fileurl;
	}
	
	public int getFinishedcount() {
		return finishedcount;
	}

	public void setFinishedcount(int finishedcount) {
		this.finishedcount = finishedcount;
	}

	public int getConvercount() {
		return convercount;
	}

	public void setConvercount(int convercount) {
		this.convercount = convercount;
	}

	public double getTotalcount() {
		return totalcount;
	}

	public void setTotalcount(double totalcount) {
		this.totalcount = totalcount;
	}

	public String getDomainname() {
		return domainname;
	}

	public void setDomainname(String domainname) {
		this.domainname = domainname;
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

	public int getFileurlhash() {
		return fileurlhash;
	}

	public void setFileurlhash(int fileurlhash) {
		this.fileurlhash = fileurlhash;
	}

	public double getSize() {
		return size;
	}

	public void setSize(double size) {
		this.size = size;
	}

	public int getDomain() {
		return domain;
	}

	public void setDomain(int domain) {
		this.domain = domain;
	}
	
}
