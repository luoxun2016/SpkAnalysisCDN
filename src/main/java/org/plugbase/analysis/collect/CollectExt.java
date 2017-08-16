package org.plugbase.analysis.collect;

import static org.plugbase.analysis.config.Configuration.DO_FINSH;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountExt;
import org.plugbase.analysis.func.lambda.ExtFunc;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectExt {
	private static final Logger log = Logger.getLogger(CollectExt.class);
	
	public static void collect(JavaRDD<CountDetail> detailData, String path){
		try {
			SQLContext sqlctx = SparkBuilder.shareSQLContext(detailData.context());
			
			JavaRDD<CountExt> extData = detailData.mapToPair(detail->ExtFunc.mapToPair(detail))
					.reduceByKey((v1,v2)->ExtFunc.reduceByKey(v1, v2))
					.values();
			
			sqlctx.createDataFrame(extData, CountExt.class).write().mode(SaveMode.Overwrite).parquet(path);
			
			HadoopFileSystem.rename(path, path + DO_FINSH);
		} catch (Exception e) {
			log.error("collect ext fail",e);
		}
	}
	
}
