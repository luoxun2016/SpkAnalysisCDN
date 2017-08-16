/**
 * @version $Id: Sourcehost.java v,1.0.0 2009-9-16 上午11:52:50 WangWentong $
 * @copyright (c) 2009 北京叁加伍网络科技有限公司
 * @link http://java.plugbase.org
 * 
 * AnalysisCDNLogs 功能描述
 */
package org.plugbase.analysis.config;

/**
 * @author WangWentong
 *
 */
public class Sourcehost {
	
	private int allowdomainid;//站点id	
	private double flowrate = 1; // 流量系数
	private int serviceid;   // 站点使用业务id
	private String allowdomain; //站点域名
	private String domain; //站点子域名
	private boolean flood ; //是否是泛域名
	private int userid;
	private int agentid;
	
	public Sourcehost() {
		this.allowdomain = "";
		this.domain = "";
	}
	
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public double getFlowrate() {
		return flowrate;
	}
	public void setFlowrate(double flowrate) {
		this.flowrate = flowrate;
	}
	public int getServiceid() {
		return serviceid;
	}
	public void setServiceid(int serviceid) {
		this.serviceid = serviceid;
	}
	public String getAllowdomain() {
		return allowdomain;
	}
	public void setAllowdomain(String allowdomain) {
		this.allowdomain = allowdomain;
	}
	public void setAllowdomainid(int allowdomainid) {
		this.allowdomainid = allowdomainid;
	}
	public int getAllowdomainid() {
		return allowdomainid;
	}
	public void setFlood(boolean flood) {
		this.flood = flood;
	}
	public boolean isFlood() {
		return flood;
	}
	/**
	 * @param memberid the memberid to set
	 */
	/**
	 * @return the {@link #userid}
	 */
	public int getUserid() {
		return userid;
	}
	/**
	 * @param userid the {@link #userid} to set
	 */
	public void setUserid(int userid) {
		this.userid = userid;
	}
	/**
	 * @return the {@link #agentid}
	 */
	public int getAgentid() {
		return agentid;
	}
	/**
	 * @param agentid the {@link #agentid} to set
	 */
	public void setAgentid(int agentid) {
		this.agentid = agentid;
	}
	
}
