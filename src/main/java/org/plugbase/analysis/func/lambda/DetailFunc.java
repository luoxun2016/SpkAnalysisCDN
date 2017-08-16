package org.plugbase.analysis.func.lambda;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.plugbase.analysis.entity.CountDetail;
import org.plugbase.analysis.parser.Parser;
import org.plugbase.analysis.util.HadoopFileSystem;
import org.plugbase.analysis.util.JredisClient;

import redis.clients.jedis.Jedis;

public class DetailFunc {

	/**
	 * @param it
	 * @return
	 */
	public static Iterable<CountDetail> mapPartitionsSaveHadoop(Iterator<String> it) {
		List<CountDetail> list = new LinkedList<CountDetail>();

		Map<String, StringBuilder> map = new TreeMap<String, StringBuilder>();

		while (it.hasNext()) {
			String logline = it.next();
			CountDetail detail = Parser.parse(logline);

			if (detail == null)
				continue;

			list.add(detail);

			if (detail.getErrDomain() == null) {
				String key = new StringBuilder("/cdn/flux_log/").append(detail.getCreated()).append("/").append(detail.getDomain()).append(".log").toString();
				StringBuilder sb = map.get(key);
				if(sb == null){
					sb = new StringBuilder();
					map.put(key, sb);
				}
				
				if (sb.length() == 0) {
					sb.append(logline);
				} else {
					sb.append("\n");
					sb.append(logline);
				}
			}
		}
		
		for(Entry<String, StringBuilder> entry : map.entrySet()){
			HadoopFileSystem.writeFileString(entry.getKey(), entry.getValue().toString(), true);
		}

		return list;
	}
	
	/*
	public static Iterable<CountDetail> mapPartitionsSaveRedis(Iterator<String> it) {
		List<CountDetail> list = new LinkedList<CountDetail>();
		
		Jedis jedis = JredisClient.getInstance();
		Pipeline p = jedis.pipelined();
		
		while (it.hasNext()) {
			String logline = it.next();
			CountDetail detail = Parser.parse(logline);
			
			if (detail == null)
				continue;
			
			list.add(detail);
			
			if (detail.getErrDomain() == null) {
				p.rpush("log", logline);
			}
		}
		p.sync();
		jedis.close();
		
		return list;
	}
	*/
	
	public static Iterable<CountDetail> mapPartitionsSaveRedis(Iterator<String> it) {
		List<CountDetail> list = new LinkedList<CountDetail>();
		
		Jedis jedis = JredisClient.getInstance();
		StringBuilder sb = new StringBuilder();
		
		while (it.hasNext()) {
			String logline = it.next();
			CountDetail detail = Parser.parse(logline);
			
			if (detail == null)
				continue;
			
			list.add(detail);
			
			if (detail.getErrDomain() == null) {
				sb.append(logline);
			}
		}
		jedis.lpush("log", sb.toString());
		jedis.close();
		
		return list;
	}
}
