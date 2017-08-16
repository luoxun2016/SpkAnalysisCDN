package org.plugbase.analysis.collect.day.jdbc;

import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

import scala.Tuple2;


public abstract class CollectInfoDayAbstract<T> implements Serializable{
	private static final long serialVersionUID = -27867776207835697L;

	private static final Logger LOG = Logger.getLogger(CollectInfoDayAbstract.class);
	
	protected Date date = null;
	
	private static JavaRDD<Row> infoData = null;
	
	private static List<String> filelist = null;
	
	private static boolean isEmpty		 = false;
	
	public CollectInfoDayAbstract() {
		this(new Date());
	}
	
	public CollectInfoDayAbstract(Date date) {
		this.date = date;
	}
	
	public void collect(String inPath, String table, Class<T> clazz) {
		if(isEmpty) return;
		
		if(infoData == null){
			// 获取需要处理数据文件列表
			filelist = HadoopFileSystem.getFileList(inPath);
			
			if(filelist == null || filelist.isEmpty()){
				isEmpty = true;
				LOG.info(inPath + " not exist!");
				return;
			}

			// 获取Spark SQL对象
			SQLContext sqlctx = SparkBuilder.shareSQLContext();

			// 获取未合并的数据
			infoData = sqlctx.read()
					.parquet(filelist.toArray(new String[0]))
					.javaRDD()
					.cache();
		}
		
		try {
			// 初始化JDBC连接配置
			Properties properties = new Properties();
			properties.setProperty("user", Configuration.JDBC_USER);
			properties.setProperty("password", Configuration.JDBC_PASS);
			
			// 合并数据
			JavaPairRDD<String, T> rdd = infoData.mapToPair(row->mapToPair(row));
			rdd = reduceByKey(rdd);
			
			// 保存合并后的数据（JDBC批量插入）
			rdd.foreachPartition(it->foreachPartition(it));
			
			// 保存合并后的数据（SparkSQL插入）
//			sqlctx.createDataFrame(rdd.values(),clazz).write().mode(SaveMode.Append).jdbc(Configuration.JDBC_URL, table, properties);
			
			// 删除历史数据
			this.deleteHistoryData(table);
		} catch (Exception e) {
			LOG.error("colect " + table + " day fail",e);
		}
	}
	
	public static void destory(){
		// 释放缓存
		if(infoData != null){
			infoData.unpersist();
		}
	}
	
	protected abstract boolean deleteHistoryData(String table);
	
	protected abstract Tuple2<String, T> mapToPair(Row row);
	
	protected abstract JavaPairRDD<String, T> reduceByKey(JavaPairRDD<String, T> javaPairRDD);
	
	protected abstract void foreachPartition(Iterator<Tuple2<String, T>> it);
}
