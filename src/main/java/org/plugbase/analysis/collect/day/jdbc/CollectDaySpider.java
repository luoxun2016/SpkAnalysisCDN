package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountSpider;
import org.plugbase.analysis.func.lambda.SpiderFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDaySpider extends CollectDayAbstract<CountSpider>{
	private static final long serialVersionUID = -5618155371847479722L;

	public CollectDaySpider() {
	}
	
	public CollectDaySpider(Date date) {
		super(date);
	}

	@Override
	protected Tuple2<String, CountSpider> mapToPair(Row row) {
		return SpiderFunc.mapToPair(row);
	}

	@Override
	protected CountSpider reduceByKey(CountSpider v1, CountSpider v2) {
		return SpiderFunc.reduceByKey(v1, v2);
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
	protected void foreachPartition(Iterator<Tuple2<String, CountSpider>> it) {
		SpiderFunc.foreachPartition(it);
	}

}
