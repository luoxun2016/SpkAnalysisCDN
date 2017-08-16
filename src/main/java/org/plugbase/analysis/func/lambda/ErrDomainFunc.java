package org.plugbase.analysis.func.lambda;

import org.plugbase.analysis.entity.CountDetail;

import scala.Tuple2;

public class ErrDomainFunc {
	public static Boolean filter(CountDetail detail) throws Exception {
		return detail.getErrDomain() != null;
	}
	
	public static Tuple2<String, String> mapToPair(CountDetail detail) throws Exception {
		return new Tuple2<String, String>(detail.getErrDomain(), detail.getErrDomain());
	}
	
	public static String reduceByKey(String v1, String v2) throws Exception {
		return v1;
	}
}
