package org.plugbase.analysis.collect;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.func.lambda.ErrDomainFunc;
import org.plugbase.analysis.util.HadoopFileSystem;

public class CollectErrDomain {
	private static final Logger log = Logger.getLogger(CollectErrDomain.class);

	public static void collect(JavaRDD<CountDetail> detailData, String path) {
		try {
			List<String> list = detailData.filter(detail -> ErrDomainFunc.filter(detail))
			.mapToPair(detail -> ErrDomainFunc.mapToPair(detail))
			.reduceByKey((v1,v2)->v1)
			.values()
			.collect();
			
			if(list.size() > 0){
				StringBuilder sb = new StringBuilder();
				for(String s : list){
					sb.append(s).append("\n");
				}
				HadoopFileSystem.writeFileString(path, sb.toString());
			}
		} catch (Exception e) {
			log.error("save error domain fail",e);
		}
	}

}
