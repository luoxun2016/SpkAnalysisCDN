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
import org.plugbase.analysis.entity.CountUrl;
import org.plugbase.analysis.func.lambda.UrlFunc;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectDayUrl {
	private static final Logger log = Logger.getLogger(CollectDayUrl.class);

	public static void collect(String inPath, String outPath) {
		try {
			FileStatus[] fileStatus = HadoopFileSystem.getFiles(inPath);
			if (fileStatus == null || fileStatus.length == 0) {
				log.info(inPath + " not exist!");
				return;
			}

			SQLContext sqlctx = SparkBuilder.shareSQLContext();

			JavaPairRDD<String, CountUrl> hitData = null;
			
			if (HadoopFileSystem.isExistsFile(outPath)) {
				hitData = sqlctx.read()
						.parquet(outPath)
						.javaRDD()
						.mapToPair(row -> UrlFunc.mapToPair(row))
						.cache();
				hitData.count();
			}

			List<String> finshPaths = new ArrayList<String>();

			for (FileStatus fs : fileStatus) {
				String filePath = fs.getPath().toString();
				String newFilePath = filePath + DO_REMOVE;
				if (!HadoopFileSystem.rename(filePath, newFilePath)) continue;
				
				finshPaths.add(newFilePath);

				JavaPairRDD<String, CountUrl> tmpData = sqlctx.read()
						.parquet(newFilePath)
						.javaRDD()
						.mapToPair(row -> UrlFunc.mapToPair(row));
				
				if(hitData == null){
					hitData = tmpData;
				}else{
					hitData = hitData.union(tmpData);
				}
			}
			
			JavaRDD<CountUrl> urlDayData = hitData.reduceByKey((v1, v2) -> UrlFunc.reduceByKey(v1, v2)).values();
			
			sqlctx.createDataFrame(urlDayData, CountUrl.class).write().mode(SaveMode.Overwrite).parquet(outPath);

			for (String path : finshPaths) {
				HadoopFileSystem.delFile(path);
			}
		} catch (Exception e) {
			log.error("collect day url fail",e);
		}
	}
}
