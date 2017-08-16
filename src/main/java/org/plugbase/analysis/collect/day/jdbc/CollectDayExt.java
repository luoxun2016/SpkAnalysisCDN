package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountExt;
import org.plugbase.analysis.func.lambda.ExtFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayExt extends CollectDayAbstract<CountExt>{
	private static final long serialVersionUID = 8713651134479570357L;

	public CollectDayExt() {
	}
	
	public CollectDayExt(Date date) {
		super(date);
	}

	@Override
	protected Tuple2<String, CountExt> mapToPair(Row row) {
		return ExtFunc.mapToPair(row);
	}

	@Override
	protected CountExt reduceByKey(CountExt v1, CountExt v2) {
		return ExtFunc.reduceByKey(v1, v2);
	}

	@Override
	protected String[] historyPredicates() {
		String predicate = String.format("years=%1$tY AND months=%1$tm AND days=%1$td", this.date);
		return new String[]{predicate};
	}

	@Override
	protected boolean deleteHistoryData(String table) {
		String sql = String.format("DELETE FROM %s WHERE years=%2$tY AND months=%2$tm AND days=%2$td", table, this.date);
		try {
			DBHelper.getInstance().executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	@Override
	protected void foreachPartition(Iterator<Tuple2<String, CountExt>> it) {
		ExtFunc.foreachPartition(it);
	}
}
