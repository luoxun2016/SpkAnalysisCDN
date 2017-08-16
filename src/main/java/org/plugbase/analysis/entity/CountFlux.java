/**
 * @version $Id: Conuntdetail.java v,1.0.0 2009-9-15 下午02:54:54 WangWentong $
 * @copyright (c) 2009 北京叁加伍网络科技有限公司
 * @link http://java.plugbase.org
 * 
 * AnalysisCDNLogs 功能描述
 */
package org.plugbase.analysis.entity;

/**
 * 原cdn_年月日_daydetail表
 */
public class CountFlux {	
	private String domain;		//加速域名
	private int years;			//年
	private int months;			//月
	private int days;			//日
	private int hours;			//时
	private int minute;			//分（第几个5分钟）
	private double bandwidth;	//下行流量
	private double miss;		//未命中的下行流量
	private String cdnip;		//节点IP
	private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int serviceid;		//服务ID
	
	public CountFlux(CountDetail detail){
		this.domain = detail.getDomain();
		this.years = detail.getYear();
		this.months = detail.getMonth();
		this.days = detail.getDay();
		this.hours = detail.getHour();
		this.minute = detail.getMinute();
		this.bandwidth = detail.getBandwidth();
		this.cdnip = detail.getCdnip();
		this.mark = detail.getMark();
		this.agentid = detail.getAgentid();
		this.userid = detail.getUserid();
		this.serviceid = detail.getServiceid();
	}
	
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public int getYears() {
		return years;
	}
	public void setYears(int year) {
		this.years = year;
	}
	public int getMonths() {
		return months;
	}
	public void setMonths(int month) {
		this.months = month;
	}
	public int getDays() {
		return days;
	}
	public void setDays(int day) {
		this.days = day;
	}
	public int getHours() {
		return hours;
	}
	public void setHours(int hour) {
		this.hours = hour;
	}
	public int getMinute() {
		return minute;
	}
	public void setMinute(int minute) {
		this.minute = minute;
	}
	public double getBandwidth() {
		return bandwidth;
	}
	public void setBandwidth(double bandwidth) {
		this.bandwidth = bandwidth;
	}
	public double getMiss() {
		return miss;
	}
	public void setMiss(double miss) {
		this.miss = miss;
	}
	public String getCdnip() {
		return cdnip;
	}
	public void setCdnip(String cdnip) {
		this.cdnip = cdnip;
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
