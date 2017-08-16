package org.plugbase.analysis.util;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.log4j.Logger;
import org.plugbase.analysis.config.Configuration;

public class DBHelper {
	private final static Logger logger = Logger.getLogger(DBHelper.class);
	
	private static volatile DBHelper instance = null;
	private BasicDataSource dataSource = null;
	
	
	public static DBHelper getInstance(){
		if(instance == null)
			synchronized(DBHelper.class){
				if(instance == null) instance = new DBHelper();
			}
		return instance;
	}
	
	private DBHelper() {
		initDataSource();
	}
	
	private void initDataSource(){
		String	username = Configuration.JDBC_USER;
		String	password = Configuration.JDBC_PASS;
		String	url 	 = Configuration.JDBC_URL;
		
		logger.info(String.format("DBHelper initDataSource url:%s",url));
		
        dataSource = new BasicDataSource();
        
        //2. 为数据源实例指定必须的属性
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setUrl(url);
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        
        //1). 指定数据库连接池中初始化连接数的个数
        dataSource.setInitialSize(5);
        
        //2). 指定最大的连接数: 同一时刻可以同时向数据库申请的连接数
        dataSource.setMaxActive(5);
        
        //3). 指定小连接数: 在数据库连接池中保存的最少的空闲连接的数量 
        dataSource.setMinIdle(2);
        
        //4).等待数据库连接池分配连接的最长时间. 单位为毫秒. 超出该时间将抛出异常. 
        dataSource.setMaxWait(1000 * 5);
	}
	
	public int executeUpdate(String sql , Object...params) throws SQLException{
		QueryRunner query = new QueryRunner(dataSource);
		if(params == null){
			return query.update(sql);
		}else{
			return query.update(sql, params);
		}
	}
	
	public <T>T executeQuery(String sql , Class<T> type , Object...params) throws SQLException{
		QueryRunner query = new QueryRunner(dataSource);
		if(params == null){
			return query.query(sql, new BeanHandler<T>(type));
		}else{
			return query.query(sql, new BeanHandler<T>(type), params);
		}
	}
	
	public <T>List<T> executeQueryList(String sql , Class<T> type , Object...params) throws SQLException{
		QueryRunner query = new QueryRunner(dataSource);
		if(params == null){
			return query.query(sql, new BeanListHandler<T>(type));
		}else{
			return query.query(sql, new BeanListHandler<T>(type) , params);
		}
	}
	
	public Object[] executeQueryArray(String sql , Object...params) throws SQLException{
		QueryRunner query = new QueryRunner(dataSource);
		if(params == null){
			return query.query(sql, new ArrayHandler());
		}else{
			return query.query(sql, new ArrayHandler(), params);
		}
	}
	
	public List<Object[]> executeQueryListArray(String sql , Object...params) throws SQLException{
		QueryRunner query = new QueryRunner(dataSource);
		if(params == null){
			return query.query(sql, new ArrayListHandler());
		}else{
			return query.query(sql, new ArrayListHandler(), params);
		}
	}
	
	public int[] executeBatch(String sql, Object[][] params) throws SQLException{
		QueryRunner query = new QueryRunner(dataSource);
		return query.batch(sql, params);
	}
	
	public BigInteger executeQueryAutoIncrement(String table){
		try{
			QueryRunner query = new QueryRunner(dataSource);
			String sql = String.format("SELECT AUTO_INCREMENT FROM information_schema.tables WHERE table_name='%s'", table);
			return (BigInteger)query.query(sql, new ArrayListHandler()).get(0)[0];
		}catch(Exception e){
			logger.error("query auto increment fail", e);
			return null;
		}
	}
	
	public static void main(String[] args) throws SQLException {
		System.out.println(DBHelper.getInstance().executeQueryAutoIncrement("cdn_area"));
	}
}
