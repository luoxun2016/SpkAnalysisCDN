package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountHit;
import org.plugbase.analysis.func.lambda.HitFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayHit extends CollectDayAbstract<CountHit>{
	private static final long serialVersionUID = -8871632602222480451L;

	public CollectDayHit() {
	}
	
	public CollectDayHit(Date date) {
		super(date);
	}
	
	@Override
	protected Tuple2<String, CountHit> mapToPair(Row row) {
		return HitFunc.mapToPair(row);
	}

	@Override
	protected CountHit reduceByKey(CountHit v1, CountHit v2) {
		return HitFunc.reduceByKey(v1, v2);
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
	protected void foreachPartition(Iterator<Tuple2<String, CountHit>> it) {
		HitFunc.foreachPartition(it);
	}
}
