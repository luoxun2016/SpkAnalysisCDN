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
import org.plugbase.analysis.entity.CountExt;
import org.plugbase.analysis.func.lambda.ExtFunc;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectDayExt {
	private static final Logger log = Logger.getLogger(CollectDayExt.class);

	public static void collect(String inPath, String outPath) {
		try {
			FileStatus[] fileStatus = HadoopFileSystem.getFiles(inPath);
			if (fileStatus == null || fileStatus.length == 0) {
				log.info(inPath + " not exist!");
				return;
			}

			SQLContext sqlctx = SparkBuilder.shareSQLContext();

			JavaPairRDD<String, CountExt> extData = null;
			
			if (HadoopFileSystem.isExistsFile(outPath)) {
				extData = sqlctx.read()
						.parquet(outPath)
						.javaRDD()
						.mapToPair(row -> ExtFunc.mapToPair(row))
						.cache();
				extData.count();
			}

			List<String> finshPaths = new ArrayList<String>();

			for (FileStatus fs : fileStatus) {
				String filePath = fs.getPath().toString();
				String newFilePath = filePath + DO_REMOVE;
				if (!HadoopFileSystem.rename(filePath, newFilePath)) continue;
				
				finshPaths.add(newFilePath);

				JavaPairRDD<String, CountExt> tmpData = sqlctx.read()
						.parquet(newFilePath)
						.javaRDD()
						.mapToPair(row -> ExtFunc.mapToPair(row));
				
				if(extData == null){
					extData = tmpData;
				}else{
					extData = extData.union(tmpData);
				}
			}
			
			JavaRDD<CountExt> extDayData = extData.reduceByKey((v1, v2) -> ExtFunc.reduceByKey(v1, v2)).values();
			
			sqlctx.createDataFrame(extDayData, CountExt.class).write().mode(SaveMode.Overwrite).parquet(outPath);

			for (String path : finshPaths) {
				HadoopFileSystem.delFile(path);
			}
		} catch (Exception e) {
			log.error("collect day ext fail",e);
		}
	}
}
