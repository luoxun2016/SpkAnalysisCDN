package org.plugbase.analysis.func.lambda;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.broadcast.Broadcast;
import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.parser.PathParser;
import org.plugbase.analysis.util.HadoopFileSystem;

import scala.Tuple2;

public class FluxLogFunc {
	private static final Iterator<Tuple2<String,String>> EMTPY_ITERATOR = new ArrayList<Tuple2<String,String>>(0).iterator();

	public static Tuple2<String, String> mapToPair(String logline) {
		String filename = PathParser.parse(logline);
		if(filename != null){
			return new Tuple2<String, String>(filename,logline);
		}else{
			return null;
		}
	}

	public static boolean filter(CountDetail detail) {
		return detail.getErrDomain() == null;
	}
	
	public static Iterator<Tuple2<String,String>> mapPartitionsWithIndex(Iterator<Tuple2<String,String>> it, String rootpath) {
		StringBuilder sb = new StringBuilder();
		String filepath = null;
		
		int size = 0;
		while(it.hasNext()){
			Tuple2<String, String> tuple2 = it.next();
			sb.append(tuple2._2).append("\n");
			
			if(filepath == null){
				filepath = rootpath + tuple2._1;
			}
			
			if(size++ > 10000){
				HadoopFileSystem.writeFileString(filepath, sb.toString(), true);
				sb.delete(0, sb.length());
				size = 0;
			}
		}
		
		HadoopFileSystem.writeFileString(filepath, sb.toString(), true);
		sb.delete(0, sb.length());
		
		return EMTPY_ITERATOR;
	}
	
	public static class Partitioner extends org.apache.spark.Partitioner{
		private static final long serialVersionUID = 6882320753784621418L;
		
		Broadcast<Map<String, Long>> pathToIndexDics;
		
		public Partitioner(Broadcast<Map<String, Long>> pathToIndexDics) {
			this.pathToIndexDics = pathToIndexDics;
		}
		
		@Override
		public int numPartitions() {
			return pathToIndexDics.value().size();
		}
		
		@Override
		public int getPartition(Object key) {
			return pathToIndexDics.value().get(key).intValue();
		}
	}
}
