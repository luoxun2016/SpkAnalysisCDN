package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountStatusCode;
import org.plugbase.analysis.func.lambda.StatusCodeFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayStatusCode extends CollectInfoDayAbstract<CountStatusCode>{
	private static final long serialVersionUID = -3102621807520541541L;

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
	protected Tuple2<String, CountStatusCode> mapToPair(Row row) {
		return StatusCodeFunc.mapToPairForInfo(row);
	}

	@Override
	protected JavaPairRDD<String, CountStatusCode> reduceByKey(
			JavaPairRDD<String, CountStatusCode> javaPairRDD) {
		return javaPairRDD
				.reduceByKey((v1, v2) -> StatusCodeFunc.reduceByKey(v1, v2))
				.mapToPair(tuple2 -> StatusCodeFunc.mapToPair(tuple2))
				.reduceByKey((v1, v2) -> StatusCodeFunc.reduceByKeyWithIpcount(v1, v2));
	}

	@Override
	protected void foreachPartition(Iterator<Tuple2<String, CountStatusCode>> it) {
		StatusCodeFunc.foreachPartition(it);
	}

}
