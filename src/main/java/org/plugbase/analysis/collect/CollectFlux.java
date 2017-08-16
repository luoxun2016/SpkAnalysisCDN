package org.plugbase.analysis.collect;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountFlux;
import org.plugbase.analysis.func.lambda.FluxFunc;
import org.plugbase.analysis.util.DBHelper;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectFlux {
	private static final Logger log = Logger.getLogger(CollectFlux.class);
	
	public static void collect(JavaRDD<CountDetail> detailData, String table, String outpath){
		outpath = outpath + "/" + String.valueOf(new Date().getTime());
		try {
			JavaRDD<String> errSqlData = 
			detailData.mapToPair(detail->FluxFunc.mapToPair(detail))
				.reduceByKey((v1,v2)->FluxFunc.reduceByKey(v1, v2))
				.mapPartitions(it->FluxFunc.mapPartitions(it,table))
				.filter(v->v!=null)
				.cache();
			
			long count = errSqlData.count();
			if(count > 0){
				errSqlData.saveAsTextFile(outpath);
				HadoopFileSystem.rename(outpath, outpath + "_" + count + Configuration.DO_SQL);
			}
			errSqlData.unpersist();
		} catch (Exception e) {
			log.error("collect flux fail",e);
		}
	}
	
	public static void collect1(JavaRDD<CountDetail> detailData, String table, String output){
		try {
			JavaRDD<CountFlux> fluxData = 
					detailData.mapToPair(detail->FluxFunc.mapToPair(detail))
					.reduceByKey((v1,v2)->FluxFunc.reduceByKey(v1, v2))
					.values();
			
			SQLContext sqlContext = SparkBuilder.shareSQLContext();
			
			// 初始化JDBC连接配置
			Properties properties = new Properties();
			properties.setProperty("user", Configuration.JDBC_USER);
			properties.setProperty("password", Configuration.JDBC_PASS);
			
			DBHelper dbHelper = DBHelper.getInstance();
			
			BigInteger dbIndex = dbHelper.executeQueryAutoIncrement(table);
			
			String errorPath 		= output + "/error/" + new Date().getTime();
			String incrementPath 	= output + "/auto_increment";
			
			
			if(dbIndex == null){
				sqlContext.createDataFrame(fluxData, CountFlux.class).write().parquet(errorPath);
			}else{
				BigInteger hdfsIndex = null;
				String autoIncrement = HadoopFileSystem.readFileToString(incrementPath);
				if(autoIncrement != null){
					hdfsIndex = new BigInteger(autoIncrement);
				}
				
				if(!dbIndex.equals(hdfsIndex)){
					List<String> failList = HadoopFileSystem.getFileList(output + "/error/*");
					if(failList != null && !failList.isEmpty()){
						if(hdfsIndex != null){
							dbHelper.executeUpdate("DELETE FROM "+table+" id > ?", hdfsIndex);
						}
						
						sqlContext.read().parquet(failList.toArray(new String[0])).write().mode(SaveMode.Append).jdbc(Configuration.JDBC_URL, table, properties);
						dbIndex = dbHelper.executeQueryAutoIncrement(table);
						HadoopFileSystem.writeFileString(incrementPath, dbIndex.toString());
						HadoopFileSystem.delAllFile(failList);
					}
				}
				
				try{
					sqlContext.createDataFrame(fluxData, CountFlux.class).write().mode(SaveMode.Append).jdbc(Configuration.JDBC_URL, table, properties);
					dbIndex = dbHelper.executeQueryAutoIncrement(table);
					HadoopFileSystem.writeFileString(incrementPath, dbIndex.toString());
				}catch(Exception e){
					sqlContext.createDataFrame(fluxData, CountFlux.class).write().parquet(errorPath);
					throw new Exception(e);
				}
			}
		} catch (Exception e) {
			log.error("collect flux fail",e);
		}
	}
}
