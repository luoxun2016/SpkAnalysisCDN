package org.plugbase.analysis.collect.day.jdbc;

import static org.plugbase.analysis.config.Configuration.DO_FINSH;
import static org.plugbase.analysis.config.Configuration.DO_PROGRESS;

import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

import scala.Tuple2;

public abstract class CollectDayAbstract<T> implements Serializable{
	private static final long serialVersionUID = -4006784136438231016L;

	private static final Logger LOG = Logger.getLogger(CollectDayAbstract.class);
	
	protected Date date = null;
	
	public CollectDayAbstract() {
		this(new Date());
	}
	
	public CollectDayAbstract(Date date) {
		this.date = date;
	}
	
	public void collect(String inPath, String table, Class<T> clazz) {
		try {
			// 获取需要处理数据文件列表
			List<String> filelist = HadoopFileSystem.replaceAllFileName(inPath, DO_FINSH, DO_PROGRESS);
			
			if(filelist == null || filelist.isEmpty()){
				LOG.info(inPath + " not exist!");
				return;
			}

			// 获取Spark SQL对象
			SQLContext sqlctx = SparkBuilder.shareSQLContext();

			// 获取未合并的数据
			JavaPairRDD<String, T> rdd = sqlctx.read()
					.parquet(filelist.toArray(new String[0]))
					.javaRDD()
					.mapToPair(row -> mapToPair(row));
			
			// 初始化JDBC连接配置
			Properties properties = new Properties();
			properties.setProperty("user", Configuration.JDBC_USER);
			properties.setProperty("password", Configuration.JDBC_PASS);
			
			// 获取历史数据
			String[] predicates = this.historyPredicates();
			
			JavaPairRDD<String, T> rddDay = sqlctx.read()
					.jdbc(Configuration.JDBC_URL,table,predicates,properties)
					.javaRDD()
					.mapToPair(row -> mapToPair(row))
					.cache();
			
			// 合并历史数据
			if(rddDay.count() > 0){
				rdd.union(rddDay);
			}
			
			// 合并数据
			rdd = rdd.reduceByKey((v1, v2) -> reduceByKey(v1, v2));
			
			// 保存合并后的数据（JDBC批量插入）
			rdd.foreachPartition(it->foreachPartition(it));
			
			// 保存合并后的数据（SparkSQL插入）
//			sqlctx.createDataFrame(rdd.values(),clazz).write().mode(SaveMode.Append).jdbc(Configuration.JDBC_URL, table, properties);

			// 释放缓存
			rddDay.unpersist();
			
			// 删除处理完的数据
			for (String path : filelist) {
				HadoopFileSystem.delFile(path);
			}
			
			// 删除历史数据
			this.deleteHistoryData(table);
		} catch (Exception e) {
			LOG.error("colect " + table + " day fail",e);
		}
	}
	
	protected abstract String[] historyPredicates();
	
	protected abstract boolean deleteHistoryData(String table);
	
	protected abstract Tuple2<String, T> mapToPair(Row row);
	
	protected abstract T reduceByKey(T v1, T v2);
	
	protected abstract void foreachPartition(Iterator<Tuple2<String, T>> it);
}
