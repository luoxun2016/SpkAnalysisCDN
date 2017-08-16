package org.plugbase.analysis.collect.check;

import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.util.DBHelper;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.SparkBuilder;

public class CollectFluxCheck {
	
	public static void check(String path){
		String inpath = path + "*" + Configuration.DO_SQL;
		String outpath = path + "/" + String.valueOf(new Date().getTime());
		
		List<String> filelist = HadoopFileSystem.replaceAllFileName(inpath, Configuration.DO_SQL, Configuration.DO_PROGRESS);
		
		if(filelist == null || filelist.isEmpty()) return;
		
		inpath = inpath.replace(Configuration.DO_SQL, Configuration.DO_PROGRESS);
		
		JavaSparkContext sc = SparkBuilder.shareSparkContext();
		
		JavaRDD<String> rdd = sc.textFile(inpath)		
		.filter(sql->!sql.isEmpty())
		.map(new Function<String, String>() {
			private static final long serialVersionUID = 8186846253918707593L;

			@Override
			public String call(String sql) throws Exception {
				try {
					DBHelper dbHelper = DBHelper.getInstance();
					dbHelper.executeUpdate(sql);
					return null;
				} catch (Exception e) {
					return sql;
				}
			}
		})
		.filter(sql->sql!=null)
		.cache();
		
		long count = rdd.count();
		if(count > 0){
			rdd.saveAsTextFile(outpath);
			HadoopFileSystem.rename(outpath, outpath + "_" + count + Configuration.DO_SQL);
		}
		
		HadoopFileSystem.delAllFile(filelist);
	}
	
}
