package org.plugbase.analysis.util;

import static org.plugbase.analysis.config.Configuration.HFDS_NAME;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HadoopFileSystem {
	private static Logger logger 		= Logger.getLogger(HadoopFileSystem.class);
	private static Configuration conf 	= new Configuration();
	
	static{
		conf.set("fs.defaultFS", HFDS_NAME);
		conf.set("fs.hdfs.impl.disable.cache", "true");
	}
	
	/**
	 * 从HFDS文件系统中加载配置文件properties
	 * @param path
	 * @return
	 */
	public static Properties getProperties(String path){
		Properties props = new Properties();
		FileSystem fs = null;
		FSDataInputStream fsInputStream = null;
		try {
			try{
				fs = FileSystem.get(conf);
				fsInputStream = fs.open(new Path(path));
				InputStream inStream = fsInputStream.getWrappedStream();
				props.load(inStream);
			}finally{
				if(fsInputStream != null) fsInputStream.close();
			}
		} catch (Exception e) {
			logger.error("get properties fail",e);
		}
		return props;
	}
	
	/**
	 * 从HFDS中读取文本文件并转为字符串
	 * @param path
	 * @return
	 */
	public static String readFileToString(String path){
		StringBuilder sb = new StringBuilder();
		FileSystem fs = null;
		FSDataInputStream fsInputStream = null;
		try {
			try{
				fs = FileSystem.get(conf);
				Path f =  new Path(path);
				if(!fs.exists(f)){
					return null;
				}
				
				fsInputStream = fs.open(f);
				byte[] bytes = new byte[1024];
				for(int len = fsInputStream.read(bytes); len > 0 ;){
					sb.append(new String(bytes,0,len));
					len = fsInputStream.read(bytes);
				}
			}finally{
				if(fsInputStream != null) fsInputStream.close();
			}
		} catch (Exception e) {
			logger.error("read file to string fail",e);
		}
		return sb.toString();
	}
	
	/**
	 * HDFS写文本文件（如果文件已存在覆盖）
	 * @param file
	 * @param text
	 */
	public static void writeFileString(String file, String text){
		writeFileString(file, text, false);
	}
	
	/**
	 * HDFS写文本文件
	 * @param file
	 * @param text
	 * @param append
	 */	
	public static void writeFileString(String file, String text, boolean append){
		FileSystem fs = null;
		BufferedWriter writer = null;
		try {
			try{
				fs = FileSystem.get(conf);
				Path path = new Path(file);
				Path dir = path.getParent();
				if(!fs.exists(dir)){
					fs.mkdirs(dir);
				}
				
				FSDataOutputStream fsOutputStream = null;
				if(append){
					if(!fs.exists(path)){
						fsOutputStream = fs.create(path);
					}else{
						fsOutputStream = fs.append(path);
					}
				}else{
					fsOutputStream = fs.create(path,true);
				}
				writer = new BufferedWriter(new OutputStreamWriter(fsOutputStream));
				writer.write(text);
			}finally{
				if(writer != null) writer.close();
			}
		} catch (Exception e) {
			logger.error("write file string fail",e);
		}
	}
	
	/**
	 * 判断文件是否存在
	 * @param file
	 * @return
	 */
	public static boolean isExistsFile(String file){
		boolean isExists = false;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			isExists = fs.exists(new Path(file));
		} catch (Exception e) {
			logger.error("exists file fail",e);
		}
		return isExists;
	}
	
	/**
	 * 重命名
	 * @param path
	 * @param newpath
	 * @return
	 */
	public static boolean rename(String filePath,String newpath){
		if(filePath.equals(newpath)){
			return true;
		}
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path path = new Path(filePath);
			if(fs.exists(path)){
				if(fs.rename(path, new Path(newpath))){
					return true;
				}
			}
		} catch (Exception e) {
			logger.error("rename fail",e);
		}
		return false;
	}
	
	/**
	 * 删除文件
	 * @param path
	 * @return
	 */
	public static boolean delFile(String filePath){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path path = new Path(filePath);
			if(fs.exists(path)){
			    return fs.delete(path, true);
			}
		} catch (Exception e) {
			logger.error("delete file fail",e);
		}
		return false;
	}
	
	/**
	 * 批量删除文件
	 * @param filelist
	 */
	public static void delAllFile(List<String> filelist){
		if(filelist == null || filelist.isEmpty()) return;
		
		for(String file : filelist){
			delFile(file);
		}
	}
	
	/**
	 * 返回目录下所有文件
	 * @param path
	 */
	public static FileStatus[] getFiles(String filePath){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path path = new Path(filePath);
			if(fs.exists(path) && fs.isDirectory(path)){
				return fs.listStatus(path);
			}else{
				return fs.globStatus(path);
			}
		} catch (Exception e) {
			logger.error("get file list",e);
		}
		return null;
	}
	
	/**
	 * 
	 * @param filePath
	 * @return
	 */
	public static List<String> getFileList(String filePath){
		List<String> list = new ArrayList<String>();
		FileStatus[] fileStatus = getFiles(filePath);
		for(FileStatus status : fileStatus){
			list.add(status.getPath().toString());
		}
		return list;
	}
	
	/**
	 * 批量替换文件名
	 * @param path
	 * @param target
	 * @param replacement
	 * @return
	 */
	public static List<String> replaceAllFileName(String path , String target, String replacement){
		List<String> filelist = null;
		try{
			FileStatus[] fileStatus = getFiles(path);
			if(fileStatus == null || fileStatus.length == 0) return null;
			
			filelist = new ArrayList<String>();
			for(FileStatus status : fileStatus){
				String filePath = status.getPath().toString();
				String newpath = filePath.replace(target, replacement);
				if(rename(filePath, newpath)){
					filelist.add(newpath);
				}
			}
		}catch(Exception e){
			logger.error("replace file name fail",e);
		}
		return filelist;
	}
	
	/**
	 * 批量修改文件名
	 * @param filelist
	 * @param target
	 * @param replacement
	 */
	public static void replaceAllFileName(List<String> filelist , String target, String replacement){
		try {
			for(String filePath : filelist){
				String newpath = filePath.replace(target, replacement);
				rename(filePath, newpath);
			}
		} catch (Exception e) {
			logger.error("replace file name fail",e);
		}
	}
	
	public static void main(String[] args) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		System.out.println(fs.globStatus(new Path("/test/ddd")));
	}
}
