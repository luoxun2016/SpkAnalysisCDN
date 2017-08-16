package org.plugbase.analysis.collect;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.plugbase.analysis.func.format.MyFluxLogTextOutputFormat;
import org.plugbase.analysis.func.lambda.FluxLogFunc;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectFluxLog {
	private static final Logger log = Logger.getLogger(CollectFluxLog.class);

	public static void collect(String path, String savepath){
		try {
			JavaSparkContext sc = SparkBuilder.shareSparkContext();
			
			JavaRDD<String> logData = sc.textFile(path);
			
			collect(logData, savepath);
		} catch (Exception e) {
			log.error("collect flux log fail",e);
		}
	}
	
	public static void collect(JavaRDD<String> logData, String savepath){
		try {
			JavaSparkContext sc = JavaSparkContext.fromSparkContext(logData.context());
			// 转化日志  路径对应日志类容
			JavaPairRDD<String, String> fluxLogData = logData.mapToPair(logline->FluxLogFunc.mapToPair(logline))
					.filter(detail->(detail != null))
					.cache();
			
			Map<String, Long> map = fluxLogData.keys().distinct().zipWithIndex().collectAsMap();
			
			Broadcast<Map<String, Long>> pathToIndexDics = sc.broadcast(new HashMap<String, Long>(map));
			
			fluxLogData.partitionBy(new FluxLogFunc.Partitioner(pathToIndexDics))
			.foreachPartition(it->FluxLogFunc.mapPartitionsWithIndex(it, savepath));
		} catch (Exception e) {
			log.error("collect flux log fail",e);
		}
	}
	
	public static void collect1(JavaRDD<String> logData, String savepath){
		try {
			// 转化日志  路径对应日志类容
			JavaPairRDD<String, String> fluxLogData = logData.mapToPair(logline->FluxLogFunc.mapToPair(logline))
					.filter(detail->(detail != null));
			
			fluxLogData.saveAsHadoopFile(savepath, String.class, String.class, MyFluxLogTextOutputFormat.class);
		} catch (Exception e) {
			log.error("collect flux log fail",e);
		}
	}
}
