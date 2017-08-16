package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountDown;
import org.plugbase.analysis.func.lambda.DownFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayDown extends CollectDayAbstract<CountDown>{
	private static final long serialVersionUID = -5418311345586870056L;

	public CollectDayDown() {
	}
	
	public CollectDayDown(Date date) {
		super(date);
	}

	@Override
	protected Tuple2<String, CountDown> mapToPair(Row row) {
		return DownFunc.mapToPair(row);
	}

	@Override
	protected CountDown reduceByKey(CountDown v1, CountDown v2) {
		return DownFunc.reduceByKey(v1, v2);
	}

	@Override
	protected String[] historyPredicates() {
		String predicate = String.format("created='%1$tY%1$tm%1$td'", this.date);
		return new String[]{predicate};
	}

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
	protected void foreachPartition(Iterator<Tuple2<String, CountDown>> it) {
		DownFunc.foreachPartition(it);
	}

}
