package org.plugbase.analysis.entity;
/**
 * 原cdn_details表
 */
public class CountInfo {
	private String created;		//创建时间
	private int domain;			//域名hashcode
	private int area;			//区域
	private short isp;			//运营商
	private long ip;			//访问IP to Long
	private long hitmisscount;	//访问次数
	private int status;			//HTTP状态
	private double flux;		//下行流量
	private short hitmiss;		//是否命中：1：命中 0：未命中
	private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int serviceid;		//服务ID
	
	public CountInfo(String created, int domain, int area, short isp,
			long ip, long hitmisscount, int status, double flux,
			short hitmiss, int mark, int agentid, int userid, int serviceid) {
		super();
		this.created = created;
		this.domain = domain;
		this.area = area;
		this.isp = isp;
		this.ip = ip;
		this.hitmisscount = hitmisscount;
		this.status = status;
		this.flux = flux;
		this.hitmiss = hitmiss;
		this.mark = mark;
		this.agentid = agentid;
		this.userid = userid;
		this.serviceid = serviceid;
	}

	public CountInfo(CountDetail detail) {
		this.status = detail.getState();
		this.flux = detail.getBandwidth();
		this.domain = detail.getDomain().hashCode();
		this.hitmiss = detail.getHitmiss();
		this.mark = detail.getMark();
		this.agentid = detail.getAgentid();
		this.userid = detail.getUserid();
		this.serviceid = detail.getServiceid();
		this.isp = detail.getIsp();
		this.area = detail.getArea();
		this.ip = detail.getLongip();
		this.created = detail.getCreated();
		this.hitmisscount = 1;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public int getDomain() {
		return domain;
	}

	public void setDomain(int domaincode) {
		this.domain = domaincode;
	}

	public int getArea() {
		return area;
	}

	public void setArea(int area) {
		this.area = area;
	}

	public short getIsp() {
		return isp;
	}

	public void setIsp(short isp) {
		this.isp = isp;
	}

	public long getIp() {
		return ip;
	}

	public void setIp(long ip) {
		this.ip = ip;
	}

	public long getHitmisscount() {
		return hitmisscount;
	}

	public void setHitmisscount(long hitmisscount) {
		this.hitmisscount = hitmisscount;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int state) {
		this.status = state;
	}

	public double getFlux() {
		return flux;
	}

	public void setFlux(double flux) {
		this.flux = flux;
	}

	public short getHitmiss() {
		return hitmiss;
	}

	public void setHitmiss(short hitmiss) {
		this.hitmiss = hitmiss;
	}

	public int getMark() {
		return mark;
	}

	public void setMark(int mark) {
		this.mark = mark;
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
}
