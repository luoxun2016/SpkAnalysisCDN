package org.plugbase.analysis.collect.day.sparksql;
import static org.plugbase.analysis.config.Configuration.DO_REMOVE;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.plugbase.analysis.entity.CountIP;
import org.plugbase.analysis.func.lambda.IpFunc;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectDayIp {
	private static final Logger log = Logger.getLogger(CollectDayIp.class);

	public static void collect(String inPath, String outPath) {
		try {
			FileStatus[] fileStatus = HadoopFileSystem.getFiles(inPath);
			if (fileStatus == null || fileStatus.length == 0) {
				log.info(inPath + " not exist!");
				return;
			}

			SQLContext sqlctx = SparkBuilder.shareSQLContext();

			JavaPairRDD<String, CountIP> hitData = null;
			
			if (HadoopFileSystem.isExistsFile(outPath)) {
				hitData = sqlctx.read()
						.parquet(outPath)
						.javaRDD()
						.mapToPair(row -> IpFunc.mapToPair(row))
						.cache();
				hitData.count();
			}

			List<String> finshPaths = new ArrayList<String>();

			for (FileStatus fs : fileStatus) {
				String filePath = fs.getPath().toString();
				String newFilePath = filePath + DO_REMOVE;
				if (!HadoopFileSystem.rename(filePath, newFilePath)) continue;
				
				finshPaths.add(newFilePath);

				JavaPairRDD<String, CountIP> tmpData = sqlctx.read()
						.parquet(newFilePath)
						.javaRDD()
						.mapToPair(row -> IpFunc.mapToPair(row));
				
				if(hitData == null){
					hitData = tmpData;
				}else{
					hitData = hitData.union(tmpData);
				}
			}
			
			JavaRDD<CountIP> ipDayData = hitData.reduceByKey((v1, v2) -> IpFunc.reduceByKey(v1, v2)).values();
			
			sqlctx.createDataFrame(ipDayData, CountIP.class).write().mode(SaveMode.Overwrite).parquet(outPath);

			for (String path : finshPaths) {
				HadoopFileSystem.delFile(path);
			}
		} catch (Exception e) {
			log.error("collect day ip fail",e);
		}
	}
}
