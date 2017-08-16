package org.plugbase.analysis.streaming;

import static org.plugbase.analysis.config.Configuration.FLUX_LOG;
import static org.plugbase.analysis.config.Configuration.ROOT_PATH;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.plugbase.analysis.collect.CollectFluxLog;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.util.KafkaHelper;
import org.plugbase.analysis.util.SparkBuilder;

public class BootStrap2 {
	
	public static void main(String[] args) {
		Configuration.setAppName("CDN WriteFluxLog Streaming");
		
		JavaStreamingContext jssc = SparkBuilder.shareStreamingContext(Durations.minutes(30));
		
		// 日志保存路径
		final String hdfsLogSavePath = ROOT_PATH + "/" + FLUX_LOG + "/";
		
        // 设置kafkaParams
		final String groupId = "2";
		
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
			private static final long serialVersionUID = 7240995208421526359L;

			@Override
			public void call(JavaRDD<String> logData) throws Exception {
				CollectFluxLog.collect(logData, hdfsLogSavePath);
			}
		});
        
	
		// 存储offsets
		KafkaHelper.storeConsumerOffsets(groupId, kafkaCluster, stream);
		
		// spark streaming启动等待完成
		jssc.start();
		jssc.awaitTermination();
	}
	
}
