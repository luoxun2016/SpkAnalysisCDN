package org.plugbase.analysis.streaming;

import static org.plugbase.analysis.config.Configuration.COUNT_DOWN;
import static org.plugbase.analysis.config.Configuration.COUNT_EXT;
import static org.plugbase.analysis.config.Configuration.COUNT_FLUX;
import static org.plugbase.analysis.config.Configuration.COUNT_HIT;
import static org.plugbase.analysis.config.Configuration.COUNT_INFO;
import static org.plugbase.analysis.config.Configuration.COUNT_HTTP;
import static org.plugbase.analysis.config.Configuration.COUNT_IP;
import static org.plugbase.analysis.config.Configuration.COUNT_REFFER;
import static org.plugbase.analysis.config.Configuration.COUNT_SPIDER;
import static org.plugbase.analysis.config.Configuration.COUNT_URL;
import static org.plugbase.analysis.config.Configuration.ERROR_PATH;
import static org.plugbase.analysis.config.Configuration.ERROR_SQL;
import static org.plugbase.analysis.config.Configuration.ERR_DOMAIN;
import static org.plugbase.analysis.config.Configuration.TEMP_PATH;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.plugbase.analysis.collect.CollectDetail;
import org.plugbase.analysis.collect.CollectDown;
import org.plugbase.analysis.collect.CollectErrDomain;
import org.plugbase.analysis.collect.CollectExt;
import org.plugbase.analysis.collect.CollectFlux;
import org.plugbase.analysis.collect.CollectHit;
import org.plugbase.analysis.collect.CollectInfo;
import org.plugbase.analysis.collect.CollectHttp;
import org.plugbase.analysis.collect.CollectIp;
import org.plugbase.analysis.collect.CollectReffer;
import org.plugbase.analysis.collect.CollectSpider;
import org.plugbase.analysis.collect.CollectUrl;
import org.plugbase.analysis.collect.check.CollectFluxCheck;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.util.KafkaHelper;
import org.plugbase.analysis.util.SparkBuilder;

public class BootStrap1 {
	
	public static void main(String[] args) {
		Configuration.setAppName("CDN Analysis Streaming");
		
		JavaStreamingContext jssc = SparkBuilder.shareStreamingContext(Durations.minutes(5));
		
        // 设置kafkaParams
		final String groupId = "1";
		
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("group.id", groupId);
	    kafkaParams.put("metadata.broker.list", "spark0:9092,spark6:9092,spark7:9092,spark8:9092,spark9:9092");
	    kafkaParams.put("zookeeper.connect", "spark0:2181,spark6:2181,spark7:2181,spark8:2181,spark9:2181");
	    kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        // 创建kafka管理对象
        final KafkaCluster kafkaCluster = KafkaHelper.getKafkaCluster(kafkaParams);
	    
        // 设置话题
	    HashSet<String> topicsSet = new HashSet<String>(1);
	    topicsSet.add(Configuration.TOPIC_CDNLOG);
	    
        // 初始化offsets
        Map<TopicAndPartition, Long> fromOffsets = KafkaHelper.fromOffsets(topicsSet, kafkaParams, groupId, kafkaCluster, null);
	    
        // 创建kafkaStream
        JavaInputDStream<String> stream = KafkaUtils.createDirectStream(jssc,
                String.class, String.class, StringDecoder.class,
                StringDecoder.class, String.class, 
                kafkaParams,
                fromOffsets,
                v1->v1.message());
        
        // RDD计算
		stream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = -6794645166556394100L;

			@Override
			public void call(JavaRDD<String> javaRDD) throws Exception {
				// 处理RDD
				processForeachRdd(javaRDD);
			}				
		});
		
		// 存储offsets
		KafkaHelper.storeConsumerOffsets(groupId, kafkaCluster, stream);
	
		// spark streaming启动等待完成
		jssc.start();
		jssc.awaitTermination();
	}
	
    /**
     * 计算RDD 汇总流量、命中率、爬虫、等
     * @param javaRDD
     */
	public static void processForeachRdd(JavaRDD<String> javaRDD){
		Date now = new Date();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String created = dateFormat.format(now);
		
		String timestamp = String.valueOf(now.getTime());
		
		String hdfsErrDomainPath 		= ERROR_PATH + "/" + ERR_DOMAIN;
		
		String hdfsFluxErrPath			= ERROR_PATH + "/"+ COUNT_FLUX +"/"+ ERROR_SQL + "/";
		
		String hdfsInfoPath 			= TEMP_PATH + "/"+ COUNT_INFO +"/"+created+"/"+timestamp;
		
		String hdfsHitPath 				= TEMP_PATH + "/"+ COUNT_HIT +"/"+created+"/"+timestamp;
		
		String hdfsSpiderPath 			= TEMP_PATH + "/"+ COUNT_SPIDER +"/"+created+"/"+timestamp;
		
		String hdfsRefferPath 			= TEMP_PATH + "/"+ COUNT_REFFER +"/"+created+"/"+timestamp;
		
		String hdfsHttpPath 			= TEMP_PATH + "/"+ COUNT_HTTP +"/"+created+"/"+timestamp;
		
		String hdfsExtPath 				= TEMP_PATH + "/"+ COUNT_EXT +"/"+created+"/"+timestamp;
		
		String hdfsIpPath 				= TEMP_PATH + "/"+ COUNT_IP +"/"+created+"/"+timestamp;
		
		String hdfsUrlPath 				= TEMP_PATH + "/"+ COUNT_URL +"/"+created+"/"+timestamp;
		
		String hdfsDownPath 			= TEMP_PATH + "/"+ COUNT_DOWN +"/"+created+"/"+timestamp;
		
		// 检查流量出错的记录 恢复数据
		CollectFluxCheck.check(hdfsFluxErrPath);
		
		// 转换详情
		JavaRDD<CountDetail> detailData = CollectDetail.collect(javaRDD);
		
		// 检查流量出错的记录 恢复数据
		CollectFluxCheck.check(hdfsFluxErrPath);
		
		// 缓存详情
		detailData = detailData.persist(StorageLevel.MEMORY_ONLY_SER());
		
		// 统计错误域名
		CollectErrDomain.collect(detailData, hdfsErrDomainPath);
		
		// 统计流量
		CollectFlux.collect(detailData, "cdn_daydetail", hdfsFluxErrPath);
		
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
	}
}
