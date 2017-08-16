package org.plugbase.analysis.collect;
import static org.plugbase.analysis.config.Configuration.DO_FINSH;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.entity.CountUrl;
import org.plugbase.analysis.func.lambda.UrlFunc;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectUrl {
	private static final Logger log = Logger.getLogger(CollectUrl.class);

	public static void collect(JavaRDD<CountDetail> detailData, String path){
		try {
			SQLContext sqlctx = SparkBuilder.shareSQLContext(detailData.context());
			
			JavaRDD<CountUrl> urlData = detailData.mapToPair(detail->UrlFunc.mapToPair(detail))
					.reduceByKey((v1,v2)->UrlFunc.reduceByKey(v1, v2))
					.values();
			
			sqlctx.createDataFrame(urlData, CountUrl.class).write().mode(SaveMode.Overwrite).parquet(path);
			
			HadoopFileSystem.rename(path, path + DO_FINSH);
		} catch (Exception e) {
			log.error("collect url fail",e);
		}
	}
}
