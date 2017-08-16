package org.plugbase.analysis.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountDown;
import org.plugbase.analysis.entity.CountExt;
import org.plugbase.analysis.entity.CountFlux;
import org.plugbase.analysis.entity.CountHttp;
import org.plugbase.analysis.entity.CountIP;
import org.plugbase.analysis.entity.CountInfo;
import org.plugbase.analysis.entity.CountReffer;
import org.plugbase.analysis.entity.CountSpider;
import org.plugbase.analysis.entity.CountUrl;

public class SparkBuilder {
	
	private static volatile JavaSparkContext sc = null;
	
	private static volatile SQLContext sqlCtx = null;
	
	private static volatile JavaStreamingContext ssc;
	
	public static JavaSparkContext shareSparkContext(){
		if(sc == null){
			synchronized(SparkBuilder.class){
				if(sc == null)
					sc = builderSparkContext();
			}
		}
		return sc;
	}
	
	public static SQLContext shareSQLContext(){
		if(sqlCtx == null){
			synchronized(SparkBuilder.class){
				if(sqlCtx == null)
					sqlCtx = new SQLContext(shareSparkContext());
			}
		}
		return sqlCtx;
	}
	
	public static SQLContext shareSQLContext(SparkContext sparkContext){
		if(sqlCtx == null){
			synchronized(SparkBuilder.class){
				if(sqlCtx == null)
					sqlCtx = new SQLContext(sparkContext);
			}
		}
		return sqlCtx;
	}

	public static JavaStreamingContext shareStreamingContext(Duration duration){
		if(ssc == null){
			synchronized(SparkBuilder.class){
				if(ssc == null)
					ssc = builderStreamingContext(duration);
			}
		}
		return ssc;
	}
	
	private static SparkConf getConfig(){
		SparkConf conf = new SparkConf().setAppName(Configuration.getAppName());
		
		if(Configuration.isDebug()){
			conf.setMaster("local[2]");
			conf.set("spark.driver.host", "192.168.1.100");
			conf.set("spark.testing.memory", "2147480000");
		}
		
//		conf.set("spark.default.parallelism", "1000");
//		conf.set("spark.storage.memoryFraction", "0.7");
//		conf.set("spark.speculation", "true");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryoserializer.buffer.max", "128");
		conf.set("spark.rdd.compress","true");
		conf.set("spark.io.compression.codec", "Snappy");
		conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -verbose:gc -XX:+UseConcMarkSweepGC -XX:+UnlockDiagnosticVMOptions -XX:+UnlockExperimentalVMOptions -XX:+G1SummarizeConcMark -XX:ParallelGCThreads=10 -XX:ConcGCThreads=3");
		conf.registerKryoClasses(new Class[]{
				CountDetail.class,
				CountExt.class,
				CountDown.class,
				CountFlux.class,
				CountInfo.class,
				CountHttp.class,
				CountIP.class,
				CountReffer.class,
				CountSpider.class,
				CountUrl.class,
				});
		
		return conf;
	}
	
	private static JavaSparkContext builderSparkContext(){
		SparkConf conf = getConfig();
		
		JavaSparkContext sc 	= new JavaSparkContext(conf);
		//默认60秒
		sc.hadoopConfiguration().set("dfs.client.socket-timeout", "180000");
		//默认80秒
		sc.hadoopConfiguration().set("dfs.datanode.socket.write.timeout", "180000");
		return sc;
	}
	
	private static JavaStreamingContext builderStreamingContext( Duration duration ){
		SparkConf conf = getConfig();
		
		JavaStreamingContext sc 	= new JavaStreamingContext(conf, duration);
		//默认60秒
		sc.sparkContext().hadoopConfiguration().set("dfs.client.socket-timeout", "180000");
		//默认80秒
		sc.sparkContext().hadoopConfiguration().set("dfs.datanode.socket.write.timeout", "180000");
		return sc;
	}
}
