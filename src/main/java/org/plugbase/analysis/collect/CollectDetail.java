package org.plugbase.analysis.collect;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.parser.Parser;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectDetail {
	
	public static JavaRDD<CountDetail> collect(String path){
		JavaSparkContext sc = SparkBuilder.shareSparkContext();
		
		JavaRDD<String> logData = sc.textFile(path);
		
		return collect(logData);
	}
	
	public static JavaRDD<CountDetail> collect(JavaRDD<String> logData){
		// 转换详情
		JavaRDD<CountDetail> detailData = logData.map(logline->Parser.parse(logline)).filter(detail->(detail != null));
		
		return detailData;
	}
	
}
