package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountReffer;
import org.plugbase.analysis.func.lambda.RefferFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayReffer extends CollectDayAbstract<CountReffer>{
	private static final long serialVersionUID = 8747341425843089949L;

	public CollectDayReffer() {
	}
	
	public CollectDayReffer(Date date) {
		super(date);
	}

	@Override
	protected Tuple2<String, CountReffer> mapToPair(Row row) {
		return RefferFunc.mapToPair(row);
	}

	@Override
	protected CountReffer reduceByKey(CountReffer v1, CountReffer v2) {
		return RefferFunc.reduceByKey(v1, v2);
	}

	@Override
	protected String[] historyPredicates() {
		String predicate = String.format("created='%1$tY-%1$tm-%1$td'", this.date);
		return new String[]{predicate};
	}

	@Override
	protected boolean deleteHistoryData(String table) {
		String sql = String.format("DELETE FROM %s WHERE created='%2$tY-%2$tm-%2$td'", table, this.date);
		try {
			DBHelper.getInstance().executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	protected void foreachPartition(Iterator<Tuple2<String, CountReffer>> it) {
		RefferFunc.foreachPartition(it);
	}
}
