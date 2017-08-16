package org.plugbase.analysis.collect.day.jdbc;

import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.plugbase.analysis.entity.CountIP;
import org.plugbase.analysis.func.lambda.IpFunc;
import org.plugbase.analysis.util.DBHelper;

import scala.Tuple2;

public class CollectDayIp extends CollectDayAbstract<CountIP>{
	private static final long serialVersionUID = -8191875299509215338L;

	public CollectDayIp() {
	}
	
	public CollectDayIp(Date date) {
		super(date);
	}

	@Override
	protected Tuple2<String, CountIP> mapToPair(Row row) {
		return IpFunc.mapToPair(row);
	}

	@Override
	protected CountIP reduceByKey(CountIP v1, CountIP v2) {
		return IpFunc.reduceByKey(v1, v2);
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
	protected void foreachPartition(Iterator<Tuple2<String, CountIP>> it) {
		IpFunc.foreachPartition(it);
	}

}
