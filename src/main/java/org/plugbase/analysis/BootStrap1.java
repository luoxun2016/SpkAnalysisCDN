package org.plugbase.analysis;
import static org.plugbase.analysis.config.Configuration.COUNT_DOWN;
import static org.plugbase.analysis.config.Configuration.COUNT_EXT;
import static org.plugbase.analysis.config.Configuration.COUNT_FLUX;
import static org.plugbase.analysis.config.Configuration.COUNT_HIT;
import static org.plugbase.analysis.config.Configuration.COUNT_HTTP;
import static org.plugbase.analysis.config.Configuration.COUNT_INFO;
import static org.plugbase.analysis.config.Configuration.COUNT_IP;
import static org.plugbase.analysis.config.Configuration.COUNT_REFFER;
import static org.plugbase.analysis.config.Configuration.COUNT_SPIDER;
import static org.plugbase.analysis.config.Configuration.COUNT_URL;
import static org.plugbase.analysis.config.Configuration.DO_COLLECT;
import static org.plugbase.analysis.config.Configuration.DO_FINSH;
import static org.plugbase.analysis.config.Configuration.DO_LOG;
import static org.plugbase.analysis.config.Configuration.ERR_DOMAIN;
import static org.plugbase.analysis.config.Configuration.INPUT_PATH;
import static org.plugbase.analysis.config.Configuration.TEMP_PATH;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.plugbase.analysis.collect.CollectDetail;
import org.plugbase.analysis.collect.CollectDown;
import org.plugbase.analysis.collect.CollectErrDomain;
import org.plugbase.analysis.collect.CollectExt;
import org.plugbase.analysis.collect.CollectFlux;
import org.plugbase.analysis.collect.CollectHit;
import org.plugbase.analysis.collect.CollectHttp;
import org.plugbase.analysis.collect.CollectInfo;
import org.plugbase.analysis.collect.CollectIp;
import org.plugbase.analysis.collect.CollectReffer;
import org.plugbase.analysis.collect.CollectSpider;
import org.plugbase.analysis.collect.CollectUrl;
import org.plugbase.analysis.collect.check.CollectFluxCheck;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.util.HadoopFileSystem;

public class BootStrap1 {
	
	public static void main(String[] args) {
		Configuration.setDebug(true);
		Configuration.setAppName("CDN Analysis");
		
		Date now = new Date();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String created = dateFormat.format(now);
		
		String timestamp = String.valueOf(now.getTime());
		
		String hdfsLogPath				= INPUT_PATH + "/*.log";
		
		String hdfsErrDomainPath 		= TEMP_PATH + "/" + ERR_DOMAIN;
		
		String hdfsFluxPath				= TEMP_PATH + "/"+ COUNT_FLUX +"/"+ created + "/";
		
		String hdfsInfoPath 			= TEMP_PATH + "/"+ COUNT_INFO +"/"+created+"/"+timestamp;
		
		String hdfsHitPath 				= TEMP_PATH + "/"+ COUNT_HIT +"/"+created+"/"+timestamp;
		
		String hdfsSpiderPath 			= TEMP_PATH + "/"+ COUNT_SPIDER +"/"+created+"/"+timestamp;
		
		String hdfsRefferPath 			= TEMP_PATH + "/"+ COUNT_REFFER +"/"+created+"/"+timestamp;
		
		String hdfsHttpPath 			= TEMP_PATH + "/"+ COUNT_HTTP +"/"+created+"/"+timestamp;
		
		String hdfsExtPath 				= TEMP_PATH + "/"+ COUNT_EXT +"/"+created+"/"+timestamp;
		
		String hdfsIpPath 				= TEMP_PATH + "/"+ COUNT_IP +"/"+created+"/"+timestamp;
		
		String hdfsUrlPath 				= TEMP_PATH + "/"+ COUNT_URL +"/"+created+"/"+timestamp;
		
		String hdfsDownPath 			= TEMP_PATH + "/"+ COUNT_DOWN +"/"+created+"/"+timestamp;
		
		// 批量修改将要汇总的文件名
		List<String> filelist = HadoopFileSystem.replaceAllFileName(hdfsLogPath, DO_LOG, DO_COLLECT);
		
		if(filelist == null || filelist.isEmpty()) return;
		
		hdfsLogPath = String.join(",", filelist);
		
		// 检查流量出错的记录 恢复数据
		CollectFluxCheck.check(hdfsFluxPath);
		
		// 转换详情
		JavaRDD<CountDetail> detailData = CollectDetail.collect(hdfsLogPath);
		
		// 缓存详情
		detailData = detailData.persist(StorageLevel.MEMORY_ONLY_SER());
		
		// 统计错误域名
		CollectErrDomain.collect(detailData, hdfsErrDomainPath);
		
		// 统计流量
		CollectFlux.collect(detailData, "cdn_daydetail", hdfsFluxPath);
		
		// 统计命中率
		CollectHit.collect(detailData, hdfsHitPath);
		
		// 统计INFO（运营商，状态码，区域信息）
		CollectInfo.collect(detailData, hdfsInfoPath);
		
		// 统计爬虫
		CollectSpider.collect(detailData, hdfsSpiderPath);
		
		// 统计访问IP
		CollectIp.collect(detailData, hdfsIpPath);
		
		
		//============（以下为可选统计类容）==============
		// 统计来源
		CollectReffer.collect(detailData, hdfsRefferPath);
		
		// 统计HTTP
		CollectHttp.collect(detailData, hdfsHttpPath);
		
		// 统计扩展名
		CollectExt.collect(detailData, hdfsExtPath);
		
		// 统计URL
		CollectUrl.collect(detailData, hdfsUrlPath);
		
		// 统计下载
		CollectDown.collect(detailData, hdfsDownPath);
		
		// 批量修改汇总完成的文件名
		HadoopFileSystem.replaceAllFileName(filelist, DO_COLLECT, DO_FINSH);
	}
}
