package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountUrl;
import org.plugbase.analysis.func.lambda.UrlFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayUrl extends CollectDayAbstract<CountUrl>{
	private static final long serialVersionUID = 6784262499278471516L;

	public CollectDayUrl() {
	}
	
	public CollectDayUrl(Date date) {
		super(date);
	}

	@Override
	protected Tuple2<String, CountUrl> mapToPair(Row row) {
		return UrlFunc.mapToPair(row);
	}

	@Override
	protected CountUrl reduceByKey(CountUrl v1, CountUrl v2) {
		return UrlFunc.reduceByKey(v1, v2);
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
	protected void foreachPartition(Iterator<Tuple2<String, CountUrl>> it) {
		UrlFunc.foreachPartition(it);
	}
}
