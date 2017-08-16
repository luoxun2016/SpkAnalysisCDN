/**
 * @version $Id: Conuntdetail.java v,1.0.0 2009-9-15 下午02:54:54 WangWentong $
 * @copyright (c) 2009 北京叁加伍网络科技有限公司
 * @link http://java.plugbase.org
 * 
 * AnalysisCDNLogs 功能描述
 */
package org.plugbase.analysis.entity;

/**
 * @author WangWentong
 *
 */
public class CountDetail {
	private double bandwidth;	//文件大小byte（流量）
	private String cdnip;		//节点ip
	private int state;			//HTTP状态
	private String ip;			//访问IP
	private byte hitmiss;		//是否命中：1：命中 0：未命中
	private String reffer;		//来源
	private String spider;		//爬虫
	private String url;			//访问URL地址
	private int filetype;		//文件类型1：网页 0：其它
	private int ext;			//扩展类型1：图片 0：其它
	private int mark;			//13：国内HTTP 14：国内HTTPS 23：国外HTTP 24：国外HTTPS
	private int year;			//年
	private int month;			//月
	private int day;			//日
	private int hour;			//时
	private int minute;			//分（第几个5分钟）
	private double upflux;		//上行流量(byte)
	private int downtime;		//下行时间(ms)
	private int looptime;		//回源时间(ms)
	
	private String domain;		//域名
	private int agentid;		//代理商ID
	private int userid;			//用户ID
	private int serviceid;		//服务ID

	private int area;			//区域
	private byte isp;			//运营商
	private long longip;		//访问IP to Long
	
	private int spidertype;		//爬虫类型（google，百度）
	
	private String created;		//创建日期 年月日
	
	private String errDomain; 	//错误域名（域名加速已加节点但是有CDN日志）
	private String fluxlog;		//流量日志（提供流量日志下载）
	
	public CountDetail() {
		this.state = 200;
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

	public String getCdnip() {
		return cdnip;
	}

	public void setCdnip(String cdnip) {
		this.cdnip = cdnip;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public int getUserid() {
		return userid;
	}

	public void setUserid(int userid) {
		this.userid = userid;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public byte getHitmiss() {
		return hitmiss;
	}

	public void setHitmiss(byte hitmiss) {
		this.hitmiss = hitmiss;
	}

	public String getReffer() {
		return reffer;
	}

	public void setReffer(String reffer) {
		this.reffer = reffer;
	}

	public String getSpider() {
		return spider;
	}

	public void setSpider(String spider) {
		this.spider = spider;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public int getFiletype() {
		return filetype;
	}

	public void setFiletype(int filetype) {
		this.filetype = filetype;
	}

	public int getExt() {
		return ext;
	}

	public void setExt(int ext) {
		this.ext = ext;
	}

	public int getMark() {
		return mark;
	}

	public void setMark(int mark) {
		this.mark = mark;
	}
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		this.hour = hour;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		if(domain==null) return;
		this.domain = (domain.startsWith("."))?"*"+domain:domain;
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
	
	public double getUpflux() {
		return upflux;
	}

	public void setUpflux(double upflux) {
		this.upflux = upflux;
	}

	public int getDowntime() {
		return downtime;
	}

	public void setDowntime(int downtime) {
		this.downtime = downtime;
	}

	public int getLooptime() {
		return looptime;
	}

	public void setLooptime(int looptime) {
		this.looptime = looptime;
	}

	public int getArea() {
		return area;
	}

	public void setArea(int area) {
		this.area = area;
	}

	public byte getIsp() {
		return isp;
	}

	public void setIsp(byte isp) {
		this.isp = isp;
	}

	public long getLongip() {
		return longip;
	}

	public void setLongip(long longip) {
		this.longip = longip;
	}

	public int getSpidertype() {
		return spidertype;
	}

	public void setSpidertype(int spidertype) {
		this.spidertype = spidertype;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public String getErrDomain() {
		return errDomain;
	}

	public void setErrDomain(String errDomain) {
		this.errDomain = errDomain;
	}

	public String getFluxlog() {
		return fluxlog;
	}

	public void setFluxlog(String fluxlog) {
		this.fluxlog = fluxlog;
	}
	
}
