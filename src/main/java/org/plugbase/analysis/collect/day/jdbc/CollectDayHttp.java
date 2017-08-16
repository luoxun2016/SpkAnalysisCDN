package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountHttp;
import org.plugbase.analysis.func.lambda.HttpFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayHttp extends CollectDayAbstract<CountHttp>{
	private static final long serialVersionUID = 912938751627534986L;

	public CollectDayHttp() {
	}
	
	public CollectDayHttp(Date date) {
		super(date);
	}

	@Override
	protected Tuple2<String, CountHttp> mapToPair(Row row) {
		return HttpFunc.mapToPair(row);
	}

	@Override
	protected CountHttp reduceByKey(CountHttp v1, CountHttp v2) {
		return HttpFunc.reduceByKey(v1, v2);
	}

	@Override
	protected String[] historyPredicates() {
		String predicate = String.format("year=%1$tY AND month=%1$tm AND day=%1$td", this.date);
		return new String[]{predicate};
	}

	@Override
	protected boolean deleteHistoryData(String table) {
		String sql = String.format("DELETE FROM %s WHERE year=%2$tY AND month=%2$tm AND day=%2$td", table, this.date);
		try {
			DBHelper.getInstance().executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected void foreachPartition(Iterator<Tuple2<String, CountHttp>> it) {
		HttpFunc.foreachPartition(it);
	}
}
