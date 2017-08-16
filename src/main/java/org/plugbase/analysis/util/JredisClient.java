package org.plugbase.analysis.util;

/**
 * redis 操作工具类
 */
import org.apache.log4j.Logger;
import org.plugbase.analysis.config.Configuration;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JredisClient {
	private static final Logger LOG = Logger.getLogger(JredisClient.class);
	
	private static JedisPool jedisPool;

	static {
		try {
			String host = Configuration.getString("redis.host");
			int port 	= Configuration.getInt("redis.port");
			
			JedisPoolConfig jedisConfig = new JedisPoolConfig();
			jedisConfig.setMaxTotal(100);
			jedisConfig.setMaxIdle(30);
			jedisConfig.setTestOnBorrow(false);
			jedisPool = new JedisPool(jedisConfig, host, port);
		} catch (Exception e) {
			LOG.error("init jedis pool fail.",e);
		}
	}

	/**
	 * 从redis连接池获取redis对象实例 
	 * 
	 * @return
	 */
	public static Jedis getInstance() {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
		} catch (Exception e) {
			LOG.error("get jedis fail.",e);
		}
		return jedis;
	}
	
	/**
	 * 向redis的list集合末尾追加一批字符数据
	 * @param key
	 * @param strings
	 * @return
	 */
	public static boolean rpush(String key, String... strings){
		Jedis jedis = JredisClient.getInstance();
		if(jedis == null) return false;
		
		try{
			jedis.rpush(key, strings);
			return true;
		}catch(Exception e){
			LOG.error(e.getMessage());
			return false;
		}finally{
			jedis.close();
		}
	}
	
	/**
	 * 向redis的list集合末尾追加一批字符数据
	 * @param key
	 * @param seconds 超时时间
	 * @param strings
	 * @return
	 */
	public static boolean rpush(String key, int seconds, String... strings){
		Jedis jedis = JredisClient.getInstance();
		if(jedis == null) return false;
		
		try{
			jedis.expire(key, seconds);
			jedis.rpush(key, strings);
			return true;
		}catch(Exception e){
			LOG.error(e.getMessage());
			return false;
		}finally{
			jedis.close();
		}
	}
	
	public static void main(String[] args) {
		System.out.println(JredisClient.getInstance().llen("log"));
	}
}
