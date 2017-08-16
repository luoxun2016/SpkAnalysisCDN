package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountArea;
import org.plugbase.analysis.func.lambda.AreaFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayArea extends CollectInfoDayAbstract<CountArea> {
	private static final long serialVersionUID = -970186041650168196L;

	@Override
	protected boolean deleteHistoryData(String table) {
		String sql = String.format("DELETE FROM %s WHERE created='%2$tY%2$tm%2$td'", table, this.date);
		try {
			DBHelper.getInstance().executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected Tuple2<String, CountArea> mapToPair(Row row) {
		return AreaFunc.mapToPairForInfo(row);
	}

	@Override
	protected JavaPairRDD<String, CountArea> reduceByKey(
			JavaPairRDD<String, CountArea> javaPairRDD) {
		return javaPairRDD
				.reduceByKey((v1, v2) -> AreaFunc.reduceByKey(v1, v2))
				.mapToPair(tuple2 -> AreaFunc.mapToPair(tuple2))
				.reduceByKey((v1, v2) -> AreaFunc.reduceByKeyWithIpcount(v1, v2));
	}

	@Override
	protected void foreachPartition(Iterator<Tuple2<String, CountArea>> it) {
		AreaFunc.foreachPartition(it);
	}

}
