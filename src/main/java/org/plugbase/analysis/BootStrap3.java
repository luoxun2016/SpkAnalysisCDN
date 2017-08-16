package org.plugbase.analysis;
import static org.plugbase.analysis.config.Configuration.COUNT_HIT;
import static org.plugbase.analysis.config.Configuration.COUNT_DOWN;
import static org.plugbase.analysis.config.Configuration.COUNT_EXT;
import static org.plugbase.analysis.config.Configuration.COUNT_HTTP;
import static org.plugbase.analysis.config.Configuration.COUNT_INFO;
import static org.plugbase.analysis.config.Configuration.COUNT_IP;
import static org.plugbase.analysis.config.Configuration.COUNT_REFFER;
import static org.plugbase.analysis.config.Configuration.COUNT_SPIDER;
import static org.plugbase.analysis.config.Configuration.COUNT_URL;
import static org.plugbase.analysis.config.Configuration.TEMP_PATH;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.plugbase.analysis.collect.day.jdbc.CollectDayArea;
import org.plugbase.analysis.collect.day.jdbc.CollectDayDown;
import org.plugbase.analysis.collect.day.jdbc.CollectDayExt;
import org.plugbase.analysis.collect.day.jdbc.CollectDayHit;
import org.plugbase.analysis.collect.day.jdbc.CollectDayHttp;
import org.plugbase.analysis.collect.day.jdbc.CollectDayISP;
import org.plugbase.analysis.collect.day.jdbc.CollectDayIp;
import org.plugbase.analysis.collect.day.jdbc.CollectDayReffer;
import org.plugbase.analysis.collect.day.jdbc.CollectDaySpider;
import org.plugbase.analysis.collect.day.jdbc.CollectDayStatusCode;
import org.plugbase.analysis.collect.day.jdbc.CollectDayUrl;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.entity.CountArea;
import org.plugbase.analysis.entity.CountDown;
import org.plugbase.analysis.entity.CountExt;
import org.plugbase.analysis.entity.CountHit;
import org.plugbase.analysis.entity.CountHttp;
import org.plugbase.analysis.entity.CountIP;
import org.plugbase.analysis.entity.CountISP;
import org.plugbase.analysis.entity.CountReffer;
import org.plugbase.analysis.entity.CountSpider;
import org.plugbase.analysis.entity.CountStatusCode;
import org.plugbase.analysis.entity.CountUrl;

public class BootStrap3 {
	
	public static void main(String[] args) {
		Configuration.setDebug(true);
		Configuration.setAppName("CDN Analysis Day");
		
		Date now = new Date();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String created = dateFormat.format(now);
		
		String hdfsHitPath 			= TEMP_PATH + "/"+ COUNT_HIT +"/"+created+"/"+"*.finsh";
		
		String hdfsInfoPath 		= TEMP_PATH + "/"+ COUNT_INFO +"/"+created+"/"+"*.finsh";
		
		String hdfsSpiderPath 		= TEMP_PATH + "/"+ COUNT_SPIDER +"/"+created+"/"+"*.finsh";
		
		String hdfsRefferPath 		= TEMP_PATH + "/"+ COUNT_REFFER +"/"+created+"/"+"*.finsh";
		
		String hdfsHttpPath 		= TEMP_PATH + "/"+ COUNT_HTTP +"/"+created+"/"+"*.finsh";
		
		String hdfsExtPath 			= TEMP_PATH + "/"+ COUNT_EXT +"/"+created+"/"+"*.finsh";
		
		String hdfsIpPath 			= TEMP_PATH + "/"+ COUNT_IP +"/"+created+"/"+"*.finsh";
		
		String hdfsUrlPath 			= TEMP_PATH + "/"+ COUNT_URL +"/"+created+"/"+"*.finsh";
		
		String hdfsDownPath 		= TEMP_PATH + "/"+ COUNT_DOWN +"/"+created+"/"+"*.finsh";
		
		// 统计命中率
		CollectDayHit collectDayHit = new CollectDayHit();
		collectDayHit.collect(hdfsHitPath, "cdn_hit", CountHit.class);
		
		// 统计区域
		CollectDayArea collectDayArea = new CollectDayArea();
		collectDayArea.collect(hdfsInfoPath, "cdn_area", CountArea.class);
		
		// 统计运营商
		CollectDayISP collectDayISP = new CollectDayISP();
		collectDayISP.collect(hdfsInfoPath, "cdn_isp", CountISP.class);
		
		// 统计状态码
		CollectDayStatusCode collectDayStatusCode = new CollectDayStatusCode();
		collectDayStatusCode.collect(hdfsInfoPath, "cdn_statuscode", CountStatusCode.class);
		
		// 统计访问IP
		CollectDayIp collectDayIp = new CollectDayIp();
		collectDayIp.collect(hdfsIpPath, "cdn_ip", CountIP.class);
		
		// 统计爬虫
		CollectDaySpider collectDaySpider = new CollectDaySpider();
		collectDaySpider.collect(hdfsSpiderPath, "cdn_spider", CountSpider.class);
		
		
		//============（以下为可选统计类容）==============
		// 统计来源
		CollectDayReffer collectDayReffer = new CollectDayReffer();
		collectDayReffer.collect(hdfsRefferPath, "cdn_reffer", CountReffer.class);
		
		// 统计HTTP
		CollectDayHttp collectDayHttp = new CollectDayHttp();
		collectDayHttp.collect(hdfsHttpPath, "cdn_http", CountHttp.class);
		
		// 统计扩展名
		CollectDayExt collectDayExt = new CollectDayExt();
		collectDayExt.collect(hdfsExtPath, "cdn_ext", CountExt.class);
		
		// 统计URL
		CollectDayUrl collectDayUrl = new CollectDayUrl();
		collectDayUrl.collect(hdfsUrlPath, "cdn_url", CountUrl.class);
		
		// 统计下载
		CollectDayDown collectDayDown = new CollectDayDown();
		collectDayDown.collect(hdfsDownPath, "cdn_download", CountDown.class);
	}
}
