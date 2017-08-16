package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountISP;
import org.plugbase.analysis.func.lambda.ISPFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayISP extends CollectInfoDayAbstract<CountISP>{
	private static final long serialVersionUID = -5759597614874808189L;

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
	protected Tuple2<String, CountISP> mapToPair(Row row) {
		return ISPFunc.mapToPairForInfo(row);
	}

	@Override
	protected JavaPairRDD<String, CountISP> reduceByKey(
			JavaPairRDD<String, CountISP> javaPairRDD) {
		return javaPairRDD
				.reduceByKey((v1, v2) -> ISPFunc.reduceByKey(v1, v2))
				.mapToPair(tuple2 -> ISPFunc.mapToPair(tuple2))
				.reduceByKey((v1, v2) -> ISPFunc.reduceByKeyWithIpcount(v1, v2));
	}

	@Override
	protected void foreachPartition(Iterator<Tuple2<String, CountISP>> it) {
		ISPFunc.foreachPartition(it);
	}

}
