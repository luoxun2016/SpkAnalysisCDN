package org.plugbase.analysis;

import org.plugbase.analysis.util.DBHelper;
import org.plugbase.analysis.util.HadoopFileSystem;



public class Example {
	
	public static void main(String[] args) throws Exception {
		DBHelper.getInstance().executeUpdate("delete from cdn_daydetail");
		DBHelper.getInstance().executeUpdate("delete from cdn_download");
		DBHelper.getInstance().executeUpdate("delete from cdn_ext");
		DBHelper.getInstance().executeUpdate("delete from cdn_hit");
		DBHelper.getInstance().executeUpdate("delete from cdn_http");
		DBHelper.getInstance().executeUpdate("delete from cdn_ip");
		DBHelper.getInstance().executeUpdate("delete from cdn_reffer");
		DBHelper.getInstance().executeUpdate("delete from cdn_spider");
		DBHelper.getInstance().executeUpdate("delete from cdn_url");
		DBHelper.getInstance().executeUpdate("delete from cdn_area");
		DBHelper.getInstance().executeUpdate("delete from cdn_isp");
		DBHelper.getInstance().executeUpdate("delete from cdn_statuscode");
		HadoopFileSystem.rename("/cdn/input/access.remove", "/cdn/input/access.log");
		HadoopFileSystem.rename("/cdn/input/access.collect", "/cdn/input/access.log");
		HadoopFileSystem.rename("/cdn/input/access.finsh", "/cdn/input/access.log");
		HadoopFileSystem.rename("/cdn/input/access.remove", "/cdn/input/access.log");
		HadoopFileSystem.delFile("/cdn/temp");
		HadoopFileSystem.delFile("/cdn/error");
		HadoopFileSystem.delFile("/cdn/flux_log");
	}
}
